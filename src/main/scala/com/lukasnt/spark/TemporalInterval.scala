package com.lukasnt.spark

import java.time.temporal.{ChronoUnit, Temporal}

abstract class TemporalInterval[T <: Temporal](val startTime: T, val endTime: T) extends Serializable {

  def before(interval: TemporalInterval[T]): Boolean = {
    this.endTime.until(interval.startTime, ChronoUnit.NANOS) > 0
  }

  def after(interval: TemporalInterval[T]): Boolean = {
    interval.before(this)
  }

  def equals(interval: TemporalInterval[T]): Boolean = {
    this.startTime.equals(interval.startTime) && this.endTime.equals(interval.endTime)
  }

  def meets(interval: TemporalInterval[T]): Boolean = {
    this.endTime.equals(interval.startTime)
  }

  def metBy(interval: TemporalInterval[T]): Boolean = {
    interval.meets(this)
  }

  def overlaps(interval: TemporalInterval[T]): Boolean = {
    !this.before(interval) && !this.after(interval)
  }

  def overlappedBy(interval: TemporalInterval[T]): Boolean = {
    interval.overlaps(this)
  }

  def during(interval: TemporalInterval[T]): Boolean = {
    this.startTime.until(interval.startTime, ChronoUnit.NANOS) < 0 &&
      this.endTime.until(interval.endTime, ChronoUnit.NANOS) > 0
  }

  def contains(interval: TemporalInterval[T]): Boolean = {
    interval.during(this)
  }

  def starts(interval: TemporalInterval[T]): Boolean = {
    this.startTime.equals(interval.startTime) && this.endTime.until(interval.endTime, ChronoUnit.NANOS) > 0
  }

  def startedBy(interval: TemporalInterval[T]): Boolean = {
    interval.starts(this)
  }

  def finishes(interval: TemporalInterval[T]): Boolean = {
    this.startTime.until(interval.startTime, ChronoUnit.NANOS) < 0 && this.endTime.equals(interval.endTime)
  }

  def finishedBy(interval: TemporalInterval[T]): Boolean = {
    interval.finishes(this)
  }

}
