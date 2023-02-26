package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.SparkContext

import java.time.temporal.Temporal

trait TemporalGraphLoader[T <: Temporal] {
  def load(sc: SparkContext): TemporalGraph[T]

}
