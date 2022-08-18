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

import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.Random
import scala.collection.JavaConverters


object TruncTimestampCaseGenerator {

  def main(args: Array[String]) = {
    val tzIdsConfigured = System.getProperty("truncTimestamp.tzIds", "ALL")
    val tzIds: Seq[String] = tzIdsConfigured match {
      case "ALL" => JavaConverters.asScalaSetConverter(ZoneId.getAvailableZoneIds).asScala.toSeq
      case value: String => value.split(",").toSeq
    }
    val sampleSetSize = System.getProperty("truncTimestamp.sampleSetSize", "1000000").toInt

    val truncLevels = Seq("TRUNC_TO_DAY", "TRUNC_TO_HOUR")
    val tzIdLevelPairs = tzIds.flatMap(tzId => truncLevels.map(truncLevel => (tzId, truncLevel)))

    tzIdLevelPairs.foreach {
      case (tzId, truncLevelAsString) =>
        withDefaultTimeZone(ZoneId.of(tzId)) {
          generateFailingTestCases(
            tzId: String,
            truncLevelAsString: String,
            sampleSetSize,
            printlnAdapter
          )
        }
    }
  }

  def stringToLongTs(str: String, zoneId: ZoneId): Option[Long] = {
    stringToTimestamp(UTF8String.fromString(str), zoneId)
  }

  def tsToString(ts: Timestamp): String = {
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

  def generateRandomTimestamps(sampleSetSize: Int): Seq[Timestamp] = {
    val seed = System.currentTimeMillis()

    val dataGenerator =
      RandomDataGenerator.forType(
        dataType = TimestampType,
        nullable = false,
        new Random(seed)
      ).get

    val randomSamples = (0 to sampleSetSize).map(_ => dataGenerator().asInstanceOf[Timestamp])
    randomSamples
  }

  def naiveTruncExpectations(inputAsString: String, truncLevelAsString: String) = {
    val expectedAsString = truncLevelAsString match {
      case "TRUNC_TO_DAY" => s"${inputAsString.substring(0, 11)}00:00:00"
      case "TRUNC_TO_HOUR" => s"${inputAsString.substring(0, 13)}:00:00"
      case _ => throw new UnsupportedOperationException
    }
    (inputAsString, truncLevelAsString, expectedAsString)
  }

  def testTruncTimestamp(
                          zoneId: ZoneId,
                          inputAsLong: Long,
                          truncLevel: Int,
                          expectedAsLong: Long): ((ZoneId, Long, Int), Long, Long) = {

    val truncatedAsLong = DateTimeUtils.truncTimestamp(inputAsLong, truncLevel, zoneId)
    ((zoneId, inputAsLong, truncLevel), expectedAsLong, truncatedAsLong)
  }

  def generateFailingTestCases(
                                tzId: String,
                                truncLevelAsString: String,
                                sampleSetSize: Int,
                                outputAdapter: (ZoneId, Long, Int, Long, Long) => Unit
                              ) = {

    val zoneId = ZoneId.of(tzId)

    val randomTimestampSamples: Seq[Timestamp] = generateRandomTimestamps(sampleSetSize)

    val randomTestCasesWithoutTz =
      randomTimestampSamples
        .map(tsToString)
        .sorted
        .map(naiveTruncExpectations(_, truncLevelAsString))

    val testCases =
      randomTestCasesWithoutTz filter {
        case (inputAsString, truncLevelAsString, expectedAsString) =>
          val inputAsLongOption = stringToLongTs(inputAsString, zoneId)
          !inputAsLongOption.isEmpty
      } map {
        case (inputAsString, truncLevelAsString, expectedAsString) =>
          testTruncTimestamp(
            zoneId,
            stringToLongTs(inputAsString, zoneId).get,
            levelFromString(truncLevelAsString),
            stringToLongTs(expectedAsString, zoneId).get
          )
      } filter {
        case (_, expected, actual) => expected != actual
      }

    testCases.foreach {
      case ((zoneId, input, level), expected, actual) =>
        outputAdapter(zoneId, input, level, expected, actual)
    }
  }

  def printlnAdapter(
                      zoneId: ZoneId,
                      inputAsLong: Long,
                      levelAsInt: Int,
                      expectedAsLong: Long,
                      actualAsLong: Long
                    ): Unit = {
    val input = tsToString(DateTimeUtils.toJavaTimestamp(inputAsLong))
    val level = levelToString(levelAsInt)
    val expected = tsToString(DateTimeUtils.toJavaTimestamp(expectedAsLong))
    val actual = tsToString(DateTimeUtils.toJavaTimestamp(actualAsLong))
    println(f"$zoneId%s | $input%23s | $level | $expected%23s | $actual%23s | $expectedAsLong | $actualAsLong")
  }

}
