package com.lukasnt.spark

import java.time.temporal.Temporal

class TemporalProperties[T <: Temporal]
  (val interval: TemporalInterval[T], val typeLabel: String, val properties: Map[String, String])
  extends Serializable {

  def getInterval: TemporalInterval[T] = {
    this.interval
  }

  def getTypeLabel: String = {
    this.typeLabel
  }

  def getPropertyValue(key: String): Serializable = {
    this.properties.get(key)
  }

  override def toString: String = {
    s"TemporalProperty(interval=$interval, typeLabel=$typeLabel, properties=$properties)"
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TemporalProperties[T] =>
        this.interval.equals(that.interval) && this.properties.equals(that.properties)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val state = Seq(interval, properties)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
