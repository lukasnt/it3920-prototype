package com.lukasnt.spark.executors

import com.lukasnt.spark.util.PathWeightTable
import org.apache.spark.graphx.Graph

abstract class PathsConstructionExecutor[VD, ED] extends Serializable {

  def constructPaths(pregelGraph: Graph[VD, ED]): PathWeightTable

}
