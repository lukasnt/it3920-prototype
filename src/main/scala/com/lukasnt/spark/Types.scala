package com.lukasnt.spark

import org.apache.spark.graphx.Graph

import java.time.temporal.Temporal

object Types {
  type TemporalGraph[T <: Temporal] = Graph[TemporalProperties[T], TemporalProperties[T]]
}


