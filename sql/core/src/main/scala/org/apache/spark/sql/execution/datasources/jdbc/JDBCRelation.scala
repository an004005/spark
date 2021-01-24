/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.Connection

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp}
import org.apache.spark.sql.catalyst.util.StringUtils.{bigDecimalToString, getCommonPrefixLen, stringToBigDecimal, tryDivide}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, DateType, NumericType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Instructions on how to partition the table among workers.
 */
sealed abstract class JDBCPartitioningInfo[T] {
    val column: String
    val columnType: DataType
    val lowerBound: T
    val upperBound: T
    val numPartitions: Int
}

private[sql] case class JDBCPartitioningInfoLong(
    override val column: String,
    override val columnType: DataType,
    override val lowerBound: Long,
    override val upperBound: Long,
    override val numPartitions: Int)
  extends JDBCPartitioningInfo[Long]

private[sql] case class JDBCPartitioningInfoString(
    override val column: String,
    override val columnType: DataType,
    override val lowerBound: String,
    override val upperBound: String,
    override val numPartitions: Int)
  extends JDBCPartitioningInfo[String]

private[sql] case class JDBCPartitioningInfoPseudo(
    override val column: String,
    override val columnType: DataType,
    override val lowerBound: String,
    override val upperBound: String,
    override val numPartitions: Int)
  extends JDBCPartitioningInfo[String]

private[sql] object JDBCRelation extends Logging {
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   *
   * Null value predicate is added to the first partition where clause to include
   * the rows with null value for the partitions column.
   *
   * @param schema resolved schema of a JDBC table
   * @param resolver function used to determine if two identifiers are equal
   * @param timeZoneId timezone ID to be used if a partition column type is date or timestamp
   * @param jdbcOptions JDBC options that contains url
   * @return an array of partitions with where clause for each partition
   */
  def columnPartition(
      schema: StructType,
      resolver: Resolver,
      timeZoneId: String,
      jdbcOptions: JDBCOptions,
      jdbcPartitionInfo: Option[JDBCPartitioningInfo[_]] = None): Array[Partition] = {
    // if jdbcPartitionInfo set when useParallel option is set. find required values
    // otherwise, using option values
    val partitioning = jdbcPartitionInfo.getOrElse({
      import JDBCOptions._

      val partitionColumn = jdbcOptions.partitionColumn
      val lowerBound = jdbcOptions.lowerBound
      val upperBound = jdbcOptions.upperBound
      val numPartitions = jdbcOptions.numPartitions

      if (partitionColumn.isEmpty) {
        assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not " +
          s"specified, '$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
        null
      } else {
        assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty,
          s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
            s"'$JDBC_NUM_PARTITIONS' are also required")

        val (column, columnType) = verifyAndGetNormalizedPartitionColumn(
          schema, partitionColumn.get, resolver, jdbcOptions)

        columnType match {
          case _: StringType =>
            val lowerBoundValue = lowerBound.get
            val upperBoundValue = upperBound.get
            JDBCPartitioningInfoString(
              column, columnType, lowerBoundValue, upperBoundValue, numPartitions.get)
          case _: NumericType | DateType | TimestampType =>
            val lowerBoundValue = toInternalBoundValue(lowerBound.get, columnType, timeZoneId)
            val upperBoundValue = toInternalBoundValue(upperBound.get, columnType, timeZoneId)
            JDBCPartitioningInfoLong(
              column, columnType, lowerBoundValue, upperBoundValue, numPartitions.get)
          case _ =>
            null
        }
      }
    })

    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[Partition](JDBCPartition(null, 0))
    }

    partitioning match {
      case infoLong: JDBCPartitioningInfoLong =>
        getPartitionWhereClause(infoLong, timeZoneId, jdbcOptions)
      case infoString: JDBCPartitioningInfoString =>
        getPartitionWhereClause(infoString)
      case infoPseudo: JDBCPartitioningInfoPseudo =>
        getPartitionWhereClause(infoPseudo, jdbcOptions)
      case _ =>
        Array[Partition](JDBCPartition(null, 0))
    }
  }

  def getPartitionWhereClause(
      partitioning: JDBCPartitioningInfoLong,
      timeZoneId: String,
      jdbcOptions: JDBCOptions): Array[Partition] = {
    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val boundValueToString: Long => String =
      toBoundValueInWhereClause(_, partitioning.columnType, timeZoneId)
    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions || /* check for overflow */
        (upperBound - lowerBound) < 0) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; " +
          s"Lower bound: ${boundValueToString(lowerBound)}; " +
          s"Upper bound: ${boundValueToString(upperBound)}.")
        upperBound - lowerBound
      }
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.

    if (jdbcOptions.optimizePartition) {
      val partList = JdbcUtils.getSampleRowsAndPartition(
        partitioning.column, jdbcOptions, numPartitions)
      val ans = new ArrayBuffer[Partition]()
      val column = partitioning.column

      var i: Int = 0
      while (i < partList.length) {
        if (i == 0) {
          ans += JDBCPartition(s"$column < ${partList(i)} or is null", i)
        } else {
          ans += JDBCPartition(s"$column < ${partList(i)} AND $column >= ${partList(i - 1)}", i)
        }

        if (i == partList.length - 1) {
          ans += JDBCPartition(s"$column >= ${partList(i)}", i)

          i += 1
        }
      }
      val partitions = ans.toArray
      logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
        partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
      partitions
    } else {
      val stride: Long = upperBound / numPartitions - lowerBound / numPartitions

      var i: Int = 0
      val column = partitioning.column
      var currentValue = lowerBound
      val ans = new ArrayBuffer[Partition]()
      while (i < numPartitions) {
        val lBoundValue = boundValueToString(currentValue)
        val lBound = if (i != 0) s"$column >= $lBoundValue" else null
        currentValue += stride
        val uBoundValue = boundValueToString(currentValue)
        val uBound = if (i != numPartitions - 1) s"$column < $uBoundValue" else null
        val whereClause =
          if (uBound == null) {
            lBound
          } else if (lBound == null) {
            s"$uBound or $column is null"
          } else {
            s"$lBound AND $uBound"
          }
        ans += JDBCPartition(whereClause, i)
        i = i + 1
      }
      val partitions = ans.toArray
      logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
        partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
      partitions
    }
  }

  /**
   * DI Custom, make where clause for each partition with string type column
   * using Apache Sqoop's idea of org.apache.sqoop.mapreduce.db.TextSplitter
   * @param partitioning JDBCPartitioningInfo has column of string type
   * @return an array of partitions with where clause for each partition using string
   */
  def getPartitionWhereClause(
      partitioning: JDBCPartitioningInfoString): Array[Partition] = {
    val lowerString = partitioning.lowerBound
    val upperString = partitioning.upperBound

    val sharedLen = getCommonPrefixLen(lowerString, upperString)
    val commonPrefix = lowerString.substring(0, sharedLen)
    val lowerBound = stringToBigDecimal(lowerString.substring(sharedLen))
    val upperBound = stringToBigDecimal(upperString.substring(sharedLen))
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val boundValueToString: BigDecimal => String = bigDec => {
      commonPrefix + bigDecimalToString(bigDec)
    }

    val numPartitions = partitioning.numPartitions

    val stride: BigDecimal = tryDivide(upperBound - lowerBound, numPartitions)

    var i: Int = 0
    val column = partitioning.column
    var currentValue = lowerBound
    val ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBoundValue = boundValueToString(currentValue)
      val lBound = if (i != 0) s"$column >= '$lBoundValue'" else null
      currentValue += stride
      val uBoundValue = boundValueToString(currentValue)
      val uBound = if (i != numPartitions - 1) s"$column < '$uBoundValue'" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    val partitions = ans.toArray
    logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
      partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
    partitions
  }

  /**
   * DataStreams DI Team custom spark.
   * In DBMS partition read use PseudoColumn.
   * This can be support all of the case table.
   * @param partitioning JDBCPartitioningInfo has pseudo column
   * @param jdbcOptions JDBC options that contains url
   * @return an array of partitions with where clause for each partition
   */
  def getPartitionWhereClause(
      partitioning: JDBCPartitioningInfoPseudo,
      jdbcOptions: JDBCOptions): Array[Partition] = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val ans = new ArrayBuffer[Partition]()

    val numPartitions = partitioning.numPartitions
    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound

    var i: Int = 0
    while (i < numPartitions) {
      val whereClause = dialect.getPseudoColumnWhereClause(lowerBound, upperBound, numPartitions, i)
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }

    val partitions = ans.toArray
    logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
      partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
    partitions
  }

  def autoColumnPartition(
      schema: StructType,
      resolver: Resolver,
      timeZoneId: String,
      jdbcOptions: JDBCOptions): Array[Partition] = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()

    // find partition column
    val partitionColumn = dialect.supportedPseudoColumn()
      .orElse(findProperPartitionColumn(conn))
      .getOrElse({
        logWarning("Fail to find proper partition column. Using single execution")
        return Array[Partition](JDBCPartition(null, 0))
      })

    val (lowerBound, upperBound) = JdbcUtils.getMinMaxValues(conn, partitionColumn, jdbcOptions)
    val numPartitions = getNumPartitions()

    val partitioning = if (dialect.supportedPseudoColumn().nonEmpty) {
      Some(JDBCPartitioningInfoPseudo(partitionColumn, null, lowerBound, upperBound, numPartitions))
    } else {
      val (column, columnType) = verifyAndGetNormalizedPartitionColumn(
        schema, partitionColumn, resolver, jdbcOptions)

      columnType match {
        case _: StringType =>
          val lowerBoundValue = lowerBound
          val upperBoundValue = upperBound
          Some(JDBCPartitioningInfoString(
            column, columnType, lowerBoundValue, upperBoundValue, numPartitions))
        case _: NumericType | DateType | TimestampType =>
          val lowerBoundValue = toInternalBoundValue(lowerBound, columnType, timeZoneId)
          val upperBoundValue = toInternalBoundValue(upperBound, columnType, timeZoneId)
          Some(JDBCPartitioningInfoLong(
            column, columnType, lowerBoundValue, upperBoundValue, numPartitions))
        case _ =>
          null
      }
    }

    columnPartition(schema, resolver, timeZoneId, jdbcOptions, partitioning)
  }


  private def findProperPartitionColumn(conn: Connection): Option[String] = {
    // 1. use pk with high cardinality
    // 2. is not, use high cardinality column


    None
  }

  /**
   * calculate proper partition number, this is not proper.
   * Must have change to more efficient way
   * @return proper partition number
   */
  private def getNumPartitions(): Int = {
    // todo: change algorithm get more proper partition number
    try {
      val spark = SparkSession.getActiveSession.get
      val core = spark.conf.get("spark.executor.cores").toInt
      val instance = spark.conf.get("spark.executor.instances").toInt

      core*instance
    } catch {
      case _: Exception =>
        20
    }
  }

  // Verify column name and type based on the JDBC resolved schema
  private def verifyAndGetNormalizedPartitionColumn(
      schema: StructType,
      columnName: String,
      resolver: Resolver,
      jdbcOptions: JDBCOptions): (String, DataType) = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val column = schema.find { f =>
      resolver(f.name, columnName) || resolver(dialect.quoteIdentifier(f.name), columnName)
    }.getOrElse {
      val maxNumToStringFields = SQLConf.get.maxToStringFields
      throw new AnalysisException(s"User-defined partition column $columnName not " +
        s"found in the JDBC relation: ${schema.simpleString(maxNumToStringFields)}")
    }
    column.dataType match {
      case _: NumericType | DateType | TimestampType | StringType =>
      case _ =>
        throw new AnalysisException(
          s"Partition column type should be ${NumericType.simpleString}, " +
            s"${DateType.catalogString}, or ${TimestampType.catalogString}," +
            s"or ${TimestampType.catalogString}, but ${column.dataType.catalogString} found.")
    }
    (dialect.quoteIdentifier(column.name), column.dataType)
  }

  private def toInternalBoundValue(
      value: String,
      columnType: DataType,
      timeZoneId: String): Long = {
    def parse[T](f: UTF8String => Option[T]): T = {
      f(UTF8String.fromString(value)).getOrElse {
        throw new IllegalArgumentException(
          s"Cannot parse the bound value $value as ${columnType.catalogString}")
      }
    }
    columnType match {
      case _: NumericType => value.toLong
      case DateType => parse(stringToDate(_, getZoneId(timeZoneId))).toLong
      case TimestampType => parse(stringToTimestamp(_, getZoneId(timeZoneId)))
    }
  }

  private def toBoundValueInWhereClause(
      value: Long,
      columnType: DataType,
      timeZoneId: String): String = {
    def dateTimeToString(): String = {
      val dateTimeStr = columnType match {
        case DateType =>
          val dateFormatter = DateFormatter(DateTimeUtils.getZoneId(timeZoneId))
          dateFormatter.format(value.toInt)
        case TimestampType =>
          val timestampFormatter = TimestampFormatter.getFractionFormatter(
            DateTimeUtils.getZoneId(timeZoneId))
          DateTimeUtils.timestampToString(timestampFormatter, value)
      }
      s"'$dateTimeStr'"
    }
    columnType match {
      case _: NumericType => value.toString
      case DateType | TimestampType => dateTimeToString()
    }
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst schema.
   * If `customSchema` defined in the JDBC options, replaces the schema's dataType with the
   * custom schema's type.
   *
   * @param resolver function used to determine if two identifiers are equal
   * @param jdbcOptions JDBC options that contains url, table and other information.
   * @return resolved Catalyst schema of a JDBC table
   */
  def getSchema(resolver: Resolver, jdbcOptions: JDBCOptions): StructType = {
    val tableSchema = JDBCRDD.resolveTable(jdbcOptions)
    jdbcOptions.customSchema match {
      case Some(customSchema) => JdbcUtils.getCustomSchema(
        tableSchema, customSchema, resolver)
      case None => tableSchema
    }
  }

  /**
   * Resolves a Catalyst schema of a JDBC table and returns [[JDBCRelation]] with the schema.
   */
  def apply(
      parts: Array[Partition],
      jdbcOptions: JDBCOptions)(
      sparkSession: SparkSession): JDBCRelation = {
    val schema = JDBCRelation.getSchema(sparkSession.sessionState.conf.resolver, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sparkSession)
  }
}

private[sql] case class JDBCRelation(
    override val schema: StructType,
    parts: Array[Partition],
    jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  // Check if JDBCRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      filters.filter(JDBCRDD.compileFilter(_, JdbcDialects.get(jdbcOptions.url)).isEmpty)
    } else {
      filters
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(jdbcOptions.url, jdbcOptions.tableOrQuery, jdbcOptions.asProperties)
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"JDBCRelation(${jdbcOptions.tableOrQuery})" + partitioningInfo
  }
}
