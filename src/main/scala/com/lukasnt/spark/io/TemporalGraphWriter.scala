package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.GenericTemporalGraph

import java.time.temporal.Temporal

trait TemporalGraphWriter[T <: Temporal] {

  def write(graph: GenericTemporalGraph[T], path: String): Unit

}
