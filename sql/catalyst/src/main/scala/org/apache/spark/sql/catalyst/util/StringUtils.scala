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

package org.apache.spark.sql.catalyst.util

import java.math.MathContext
import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

object StringUtils extends Logging {

  /**
   * Validate and convert SQL 'like' pattern to a Java regular expression.
   *
   * Underscores (_) are converted to '.' and percent signs (%) are converted to '.*', other
   * characters are quoted literally. Escaping is done according to the rules specified in
   * [[org.apache.spark.sql.catalyst.expressions.Like]] usage documentation. An invalid pattern will
   * throw an [[AnalysisException]].
   *
   * @param pattern the SQL pattern to convert
   * @param escapeChar the escape string contains one character.
   * @return the equivalent Java regular expression of the pattern
   */
  def escapeLikeRegex(pattern: String, escapeChar: Char): String = {
    val in = pattern.toIterator
    val out = new StringBuilder()

    def fail(message: String) = throw new AnalysisException(
      s"the pattern '$pattern' is invalid, $message")

    while (in.hasNext) {
      in.next match {
        case c1 if c1 == escapeChar && in.hasNext =>
          val c = in.next
          c match {
            case '_' | '%' => out ++= Pattern.quote(Character.toString(c))
            case c if c == escapeChar => out ++= Pattern.quote(Character.toString(c))
            case _ => fail(s"the escape character is not allowed to precede '$c'")
          }
        case c if c == escapeChar => fail("it is not allowed to end with the escape character")
        case '_' => out ++= "."
        case '%' => out ++= ".*"
        case c => out ++= Pattern.quote(Character.toString(c))
      }
    }
    "(?s)" + out.result() // (?s) enables dotall mode, causing "." to match new lines
  }

  private[this] val trueStrings =
    Set("t", "true", "y", "yes", "1").map(UTF8String.fromString)

  private[this] val falseStrings =
    Set("f", "false", "n", "no", "0").map(UTF8String.fromString)

  // scalastyle:off caselocale
  def isTrueString(s: UTF8String): Boolean = trueStrings.contains(s.trimAll().toLowerCase)

  def isFalseString(s: UTF8String): Boolean = falseStrings.contains(s.trimAll().toLowerCase)
  // scalastyle:on caselocale

  /**
   * This utility can be used for filtering pattern in the "Like" of "Show Tables / Functions" DDL
   * @param names the names list to be filtered
   * @param pattern the filter pattern, only '*' and '|' are allowed as wildcards, others will
   *                follow regular expression convention, case insensitive match and white spaces
   *                on both ends will be ignored
   * @return the filtered names list in order
   */
  def filterPattern(names: Seq[String], pattern: String): Seq[String] = {
    val funcNames = scala.collection.mutable.SortedSet.empty[String]
    pattern.trim().split("\\|").foreach { subPattern =>
      try {
        val regex = ("(?i)" + subPattern.replaceAll("\\*", ".*")).r
        funcNames ++= names.filter{ name => regex.pattern.matcher(name).matches() }
      } catch {
        case _: PatternSyntaxException =>
      }
    }
    funcNames.toSeq
  }

  def ONE_PLACE: BigDecimal = BigDecimal(65536, MathContext.UNLIMITED)

  // Maximum number of characters to convert. This is to prevent rounding
  // errors or repeating fractions near the very bottom from getting out of
  // control. Note that this still gives us a huge number of possible splits.
  def MAX_CHARS: Int = 8

  def tryDivide(numerator: BigDecimal, denominator: BigDecimal): BigDecimal = {
    val jDecimal = {
      try {
        numerator.underlying().divide(denominator.underlying())
      } catch {
        case _: ArithmeticException =>
          numerator.underlying().divide(denominator.underlying(),
            java.math.BigDecimal.ROUND_UP)
      }
    }
    BigDecimal(jDecimal.toPlainString, MathContext.UNLIMITED)
  }

  /**
   * Return a BigDecimal representation of string 'str' suitable for use in a
   * numerically-sorting order.
   */
  def stringToBigDecimal(s: String): BigDecimal = {
    var curPlace = ONE_PLACE
    var result: BigDecimal = BigDecimal(0, MathContext.UNLIMITED)
    val len: Int = Math.min(s.length, MAX_CHARS)
    var i = 0

    while (i < len) {
      val codePoint = s.codePointAt(i)
      result = result + tryDivide(BigDecimal(codePoint, MathContext.UNLIMITED), curPlace)
      curPlace = curPlace * ONE_PLACE
      i += 1
    }
    result
  }

  /**
   * Return the string encoded in a BigDecimal.
   * Repeatedly multiply the input value by 65536; the integer portion after
   * such a multiplication represents a single character in base 65536.
   * Convert that back into a char and create a string out of these until we
   * have no data left.
   */
  def bigDecimalToString(bd: BigDecimal): String = {
    var cur = BigDecimal(bd.underlying().stripTrailingZeros().toString,
      MathContext.UNLIMITED)
    val sb = new StringBuilder()
    var i = 0

    while (i < MAX_CHARS) {
      cur = cur * ONE_PLACE
      val curCodePoint = cur.intValue
      if (0 == curCodePoint) return sb.toString()
      cur = cur - curCodePoint
      sb ++= Character.toChars(curCodePoint)
      i += 1
    }
    sb.toString()
  }

  /**
   * return common prefix of parameter string "lower" and "upper"
   */
  def getCommonPrefixLen(lower: String, upper: String): Int = {
    val maxPrefixLen = Math.min(lower.length, upper.length)
    var sharedLen = 0

    while (sharedLen < maxPrefixLen) {
      val c1 = lower.charAt(sharedLen)
      val c2 = upper.charAt(sharedLen)
      if (c1 != c2) {
        return sharedLen
      }
      sharedLen += 1
    }

    sharedLen
  }

  /**
   * Concatenation of sequence of strings to final string with cheap append method
   * and one memory allocation for the final string.  Can also bound the final size of
   * the string.
   */
  class StringConcat(val maxLength: Int = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
    protected val strings = new ArrayBuffer[String]
    protected var length: Int = 0

    def atLimit: Boolean = length >= maxLength

    /**
     * Appends a string and accumulates its length to allocate a string buffer for all
     * appended strings once in the toString method.  Returns true if the string still
     * has room for further appends before it hits its max limit.
     */
    def append(s: String): Unit = {
      if (s != null) {
        val sLen = s.length
        if (!atLimit) {
          val available = maxLength - length
          val stringToAppend = if (available >= sLen) s else s.substring(0, available)
          strings.append(stringToAppend)
        }

        // Keeps the total length of appended strings. Note that we need to cap the length at
        // `ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH`; otherwise, we will overflow
        // length causing StringIndexOutOfBoundsException in the substring call above.
        length = Math.min(length.toLong + sLen, ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH).toInt
      }
    }

    /**
     * The method allocates memory for all appended strings, writes them to the memory and
     * returns concatenated string.
     */
    override def toString: String = {
      val finalLength = if (atLimit) maxLength else length
      val result = new java.lang.StringBuilder(finalLength)
      strings.foreach(result.append)
      result.toString
    }
  }

  /**
   * A string concatenator for plan strings.  Uses length from a configured value, and
   *  prints a warning the first time a plan is truncated.
   */
  class PlanStringConcat extends StringConcat(Math.max(0, SQLConf.get.maxPlanStringLength - 30)) {
    override def toString: String = {
      if (atLimit) {
        logWarning(
          "Truncated the string representation of a plan since it was too long. The " +
            s"plan had length ${length} and the maximum is ${maxLength}. This behavior " +
            s"can be adjusted by setting '${SQLConf.MAX_PLAN_STRING_LENGTH.key}'.")
        val truncateMsg = if (maxLength == 0) {
          s"Truncated plan of $length characters"
        } else {
          s"... ${length - maxLength} more characters"
        }
        val result = new java.lang.StringBuilder(maxLength + truncateMsg.length)
        strings.foreach(result.append)
        result.append(truncateMsg)
        result.toString
      } else {
        super.toString
      }
    }
  }
}
