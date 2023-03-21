package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.GenericTemporalGraph
import org.apache.spark.SparkContext

import java.time.temporal.Temporal

trait TemporalGraphLoader[T <: Temporal] {
  
  def load(sc: SparkContext): GenericTemporalGraph[T]

}
