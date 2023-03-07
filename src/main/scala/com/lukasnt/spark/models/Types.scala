package com.lukasnt.spark.models

import org.apache.spark.graphx.Graph

import java.time.temporal.Temporal

object Types {
  type TemporalGraph[T <: Temporal] =
    Graph[TemporalProperties[T], TemporalProperties[T]]

  type TemporalPregelGraph[T <: Temporal] =
    Graph[(TemporalProperties[T], List[PathQueryState]), TemporalProperties[T]]
}
