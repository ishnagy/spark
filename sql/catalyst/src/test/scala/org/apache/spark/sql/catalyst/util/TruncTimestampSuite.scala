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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.matchers.must.Matchers

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.ZoneId
import scala.io.Source


class TruncTimestampSuite extends SparkFunSuite with Matchers with SQLHelper {

  private def stringToLongTs(str: String, zoneId: ZoneId): Option[Long] = {
    stringToTimestamp(UTF8String.fromString(str), zoneId)
  }

  private def tsToString(ts: Timestamp): String = {
    // the SimpleDateFormat instance implicitly depends on the default TZ,
    // always recreating a new instance to prevent caching a stale value
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    formatter.format(ts)
  }

  def levelToString(level: Int): String = {
    level match {
      case TRUNC_INVALID => "TRUNC_INVALID"
      case TRUNC_TO_MICROSECOND => "TRUNC_TO_MICROSECOND"
      case TRUNC_TO_MILLISECOND => "TRUNC_TO_MILLISECOND"
      case TRUNC_TO_SECOND => "TRUNC_TO_SECOND"
      case TRUNC_TO_MINUTE => "TRUNC_TO_MINUTE"
      case TRUNC_TO_HOUR => "TRUNC_TO_HOUR"
      case TRUNC_TO_DAY => "TRUNC_TO_DAY"
      case TRUNC_TO_WEEK => "TRUNC_TO_WEEK"
      case TRUNC_TO_MONTH => "TRUNC_TO_MONTH"
      case TRUNC_TO_QUARTER => "TRUNC_TO_QUARTER"
      case TRUNC_TO_YEAR => "TRUNC_TO_YEAR"
    }
  }

  def levelFromString(level: String): Int = {
    level match {
      case "TRUNC_INVALID" => TRUNC_INVALID
      case "TRUNC_TO_MICROSECOND" => TRUNC_TO_MICROSECOND
      case "TRUNC_TO_MILLISECOND" => TRUNC_TO_MILLISECOND
      case "TRUNC_TO_SECOND" => TRUNC_TO_SECOND
      case "TRUNC_TO_MINUTE" => TRUNC_TO_MINUTE
      case "TRUNC_TO_HOUR" => TRUNC_TO_HOUR
      case "TRUNC_TO_DAY" => TRUNC_TO_DAY
      case "TRUNC_TO_WEEK" => TRUNC_TO_WEEK
      case "TRUNC_TO_MONTH" => TRUNC_TO_MONTH
      case "TRUNC_TO_QUARTER" => TRUNC_TO_QUARTER
      case "TRUNC_TO_YEAR" => TRUNC_TO_YEAR
    }
  }

  def loadTestCasesFromResource(resourcePath: String): Seq[(String, String, String, String)] = {
    val resource = Source.fromResource(resourcePath)
    val lines: Iterator[String] = resource.getLines
    lines.map { line =>
      val testCase = line.split("\\|")

      val tzId = testCase(0).trim
      val inputAsString = testCase(1).trim
      val truncLevelAsString = testCase(2).trim
      val expectedAsString = testCase(3).trim

      (tzId, inputAsString, truncLevelAsString, expectedAsString)
    }.toSeq
  }

  def testTruncTimestamp(
                          zoneId: ZoneId,
                          inputAsLong: Long,
                          truncLevel: Int,
                          expectedAsLong: Long): ((ZoneId, Long, Int), Long, Long) = {

    val truncatedAsLong = DateTimeUtils.truncTimestamp(inputAsLong, truncLevel, zoneId)
    ((zoneId, inputAsLong, truncLevel), expectedAsLong, truncatedAsLong)
  }

  def defineTestCase(
                      tzId: String,
                      inputAsString: String,
                      truncLevelAsString: String,
                      expectedAsString: String) = {

    test(s"$tzId, '$inputAsString', $truncLevelAsString") {
      val zoneId = ZoneId.of(tzId)
      withDefaultTimeZone(zoneId) {
        val truncLevelAsInt = levelFromString(truncLevelAsString)
        val inputAsLong = stringToLongTs(inputAsString, zoneId).getOrElse(Long.MinValue)
        assume(inputAsLong != Long.MinValue)
        val expectedAsLong = stringToLongTs(expectedAsString, zoneId).getOrElse(Long.MinValue)
        assume(expectedAsLong != Long.MinValue)

        val testResult = testTruncTimestamp(zoneId, inputAsLong, truncLevelAsInt, expectedAsLong)

        val expected = tsToString(DateTimeUtils.toJavaTimestamp(testResult._2))
        val actual = tsToString(DateTimeUtils.toJavaTimestamp(testResult._3))

        assert(expected === actual)
      }
    }
  }

  val testCases = loadTestCasesFromResource("org/apache/spark/sql/catalyst/util/TruncTimestampSuite.batch0.txt")

  testCases.foreach {
    case (tzId: String, inputAsString: String, truncLevelAsString: String, expectedAsString: String)
    => defineTestCase(tzId, inputAsString, truncLevelAsString, expectedAsString)
  }

}

