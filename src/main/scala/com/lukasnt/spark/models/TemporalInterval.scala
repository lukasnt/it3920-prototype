package com.lukasnt.spark.models

import java.time.temporal.{ChronoUnit, Temporal}

/**
  * TemporalInterval is an class that represents a temporal interval.
  * @param startTime start time of the interval
  * @param endTime end time of the interval
  * @tparam T Temporal type of the interval (e.g. LocalDateTime, LocalTime, etc.)
  */
class TemporalInterval[T <: Temporal](val startTime: T, val endTime: T) extends Serializable {

  override def equals(obj: Any): Boolean = {
    if (this.startTime == null && this.endTime == null) {
      if (obj == null) true
      else
        obj match {
          case interval: TemporalInterval[T] => interval.startTime == null && interval.endTime == null
          case _                             => false
        }
    } else
      obj match {
        case interval: TemporalInterval[T] => this.equals(interval)
        case _                             => false
      }
  }

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

  /**
    * @param interval interval to get the intersection with
    * @return the intersected interval
    */
  def intersection(interval: TemporalInterval[T]): TemporalInterval[T] = {
    if (this.during(interval)) {
      this
    } else if (interval.during(this)) {
      interval
    } else if (this.startTime.until(interval.startTime, ChronoUnit.NANOS) > 0) {
      TemporalInterval(interval.startTime, this.endTime)
    } else {
      TemporalInterval(this.startTime, interval.endTime)
    }
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
    * @param interval interval to get the union with
    * @return the union interval
    */
  def union(interval: TemporalInterval[T]): TemporalInterval[T] = {
    TemporalInterval(
      if (this.startTime.until(interval.startTime, ChronoUnit.NANOS) > 0) this.startTime else interval.startTime,
      if (this.endTime.until(interval.endTime, ChronoUnit.NANOS) < 0) this.endTime else interval.endTime
    )
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval overlaps the given interval, false otherwise
    */
  def overlaps(interval: TemporalInterval[T]): Boolean = {
    this.startTime.until(interval.endTime, ChronoUnit.NANOS) > 0 &&
    this.endTime.until(interval.startTime, ChronoUnit.NANOS) < 0
  }

  /**
    * @param interval interval to compare with
    * @return true if this interval is before the given interval, false otherwise
    */
  def before(interval: TemporalInterval[T]): Boolean = {
    this.endTime.until(interval.startTime, ChronoUnit.NANOS) >= 0
  }

  /**
    * @return the duration of the interval in ChronoUnit.NANOS
    */
  def getDuration: Long = {
    this.startTime.until(this.endTime, ChronoUnit.NANOS)
  }

  def isNullInterval: Boolean = {
    this == TemporalInterval()
  }

  override def toString: String = {
    s"Interval($startTime, $endTime)"
  }

}

object TemporalInterval {

  def apply[T <: Temporal](startTime: T, endTime: T): TemporalInterval[T] = {
    new TemporalInterval[T](startTime, endTime)
  }

  def apply[T <: Temporal](): TemporalInterval[T] = {
    new TemporalInterval[T](null.asInstanceOf[T], null.asInstanceOf[T])
  }

}
