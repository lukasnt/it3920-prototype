package com.lukasnt.spark.models

import org.apache.spark.graphx.Graph

import java.time.ZonedDateTime
import java.time.temporal.Temporal

/**
  * Type aliases for the models classes.
  * ZonedDateTime is used as the default temporal type.
  */
object Types {

  type Interval = TemporalInterval[ZonedDateTime]

  type Properties = TemporalProperties[ZonedDateTime]

  type GenericTemporalGraph[T <: Temporal] = Graph[TemporalProperties[T], TemporalProperties[T]]

  type TemporalGraph = GenericTemporalGraph[ZonedDateTime]

  type TemporalPregelGraph = Graph[(Properties, List[QueryState]), Properties]

  trait PathQuery {}

}
