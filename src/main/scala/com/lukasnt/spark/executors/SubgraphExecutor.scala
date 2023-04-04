package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.graphx.Graph

abstract class SubgraphExecutor[VD, ED] extends Serializable {

  def subgraph(temporalGraph: TemporalGraph): Graph[VD, ED]

}
