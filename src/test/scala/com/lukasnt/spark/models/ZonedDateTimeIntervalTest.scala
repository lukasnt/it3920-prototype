package com.lukasnt.spark.models

import org.junit.Test

import java.time.ZonedDateTime

@Test
class ZonedDateTimeIntervalTest {

  @Test
  def standardIntersection(): Unit = {
    val interval1 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z"))
    val interval2 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:30Z"), ZonedDateTime.parse("2019-01-01T00:01:30Z"))
    val intersection = interval1.intersection(interval2)
    assert(intersection.startTime == ZonedDateTime.parse("2019-01-01T00:00:30Z"))
    assert(intersection.endTime == ZonedDateTime.parse("2019-01-01T00:01:00Z"))
  }

  @Test
  def duringIntersection(): Unit = {
    val interval1 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z"))
    val interval2 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:30Z"), ZonedDateTime.parse("2019-01-01T00:00:45Z"))
    val intersection = interval1.intersection(interval2)
    assert(intersection.startTime == ZonedDateTime.parse("2019-01-01T00:00:30Z"))
    assert(intersection.endTime == ZonedDateTime.parse("2019-01-01T00:00:45Z"))
  }

  @Test
  def standardUnion(): Unit = {
    val interval1 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z"))
    val interval2 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:30Z"), ZonedDateTime.parse("2019-01-01T00:01:30Z"))
    val union = interval1.union(interval2)
    assert(union.startTime == ZonedDateTime.parse("2019-01-01T00:00:00Z"))
    assert(union.endTime == ZonedDateTime.parse("2019-01-01T00:01:30Z"))
  }

  @Test
  def duringUnion(): Unit = {
    val interval1 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z"))
    val interval2 =
      new TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:30Z"), ZonedDateTime.parse("2019-01-01T00:00:45Z"))
    val union = interval1.union(interval2)
    assert(union.startTime == ZonedDateTime.parse("2019-01-01T00:00:00Z"))
    assert(union.endTime == ZonedDateTime.parse("2019-01-01T00:01:00Z"))
  }

}
