package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.graphx.Graph

abstract class SubgraphExecutor extends Serializable {

  def subgraph(temporalGraph: TemporalGraph): TemporalGraph

}
