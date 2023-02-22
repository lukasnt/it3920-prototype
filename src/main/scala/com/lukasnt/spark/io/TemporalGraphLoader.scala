package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.SparkContext

import java.time.temporal.Temporal

trait TemporalGraphLoader {
  def readEdgeListFiles[T <: Temporal](
      sc: SparkContext,
      labelFiles: Map[String, String]): TemporalGraph[T]

}
