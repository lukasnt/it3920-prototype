package com.lukasnt.spark.models

import java.time.temporal.Temporal

/**
  * Represents properties of a certain label-type with an temporal interval attached to it.
  * This class is used to represent the vertex or edge properties of a temporal graph.
  * @param interval valid interval of the properties-values
  * @param typeLabel string indicating label-type
  * @param properties properties with their values as a map of key-value pairs
  * @tparam T Temporal type of the interval (e.g. LocalDateTime, LocalTime, etc.)
  */
class TemporalProperties[T <: Temporal](val interval: TemporalInterval[T],
                                        val typeLabel: String,
                                        val properties: Map[String, String])
    extends Serializable {

  /**
    * @return interval of the properties
    */
  def getInterval: TemporalInterval[T] = {
    this.interval
  }

  /**
    * @return type-label of the properties
    */
  def getTypeLabel: String = {
    this.typeLabel
  }

  /**
    * @param key key of the property
    * @return value of the property with the given key
    */
  def getPropertyValue(key: String): Serializable = {
    this.properties.get(key)
  }

  override def toString: String = {
    s"TemporalProperty(interval=$interval, typeLabel=$typeLabel, properties=$properties)"
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TemporalProperties[T] =>
        this.interval.equals(that.interval) &&
          this.typeLabel.equals(that.typeLabel) &&
          this.properties.equals(that.properties)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val state = Seq(interval, typeLabel, properties)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
