package com.lukasnt.spark.models

import java.time.temporal.{ChronoUnit, Temporal}

/**
  * TemporalInterval is an class that represents a temporal interval.
  * @param startTime start time of the interval
  * @param endTime end time of the interval
  * @tparam T Temporal type of the interval (e.g. LocalDateTime, LocalTime, etc.)
  */
class TemporalInterval[T <: Temporal](val startTime: T, val endTime: T)
    extends Serializable {

  /**
    * @param interval interval to compare with
    * @return true if this interval is equal the given interval, false otherwise
    */
  def equals(interval: TemporalInterval[T]): Boolean = {
    this.startTime.equals(interval.startTime) &&
    this.endTime.equals(interval.endTime)
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval meets the given interval, false otherwise
    */
  def meets(interval: TemporalInterval[T]): Boolean = {
    this.endTime.equals(interval.startTime)
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval overlaps the given interval, false otherwise
    */
  def overlaps(interval: TemporalInterval[T]): Boolean = {
    !this.before(interval) && !interval.before(this)
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval is before the given interval, false otherwise
    */
  def before(interval: TemporalInterval[T]): Boolean = {
    this.endTime.until(interval.startTime, ChronoUnit.NANOS) > 0
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval is during the given interval, false otherwise
    */
  def during(interval: TemporalInterval[T]): Boolean = {
    this.startTime.until(interval.startTime, ChronoUnit.NANOS) < 0 &&
    this.endTime.until(interval.endTime, ChronoUnit.NANOS) > 0
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval starts the given interval, false otherwise
    */
  def starts(interval: TemporalInterval[T]): Boolean = {
    this.startTime.equals(interval.startTime) &&
    this.endTime.until(interval.endTime, ChronoUnit.NANOS) > 0
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval finishes the given interval, false otherwise
    */
  def finishes(interval: TemporalInterval[T]): Boolean = {
    this.startTime.until(interval.startTime, ChronoUnit.NANOS) < 0 &&
    this.endTime.equals(interval.endTime)
  }

  override def toString: String = {
    s"TemporalInterval($startTime, $endTime)"
  }
}
