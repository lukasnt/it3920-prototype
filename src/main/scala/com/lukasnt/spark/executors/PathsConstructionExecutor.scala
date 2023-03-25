package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

abstract class PathsConstructionExecutor[VD, ED] extends Serializable {

  def constructPaths(pregelGraph: Graph[VD, ED]): RDD[TemporalPath]

}
