package com.lukasnt.spark.examples

import com.lukasnt.spark.models.Types.TemporalGraph

object SimpleSNBGraphs {

  val temporalGraph: TemporalGraph = SimpleSNBLoader.load()

  def personKnowsGraph(): TemporalGraph = {
    temporalGraph.subgraph(vpred = (id, attr) => attr.typeLabel == "Person",
                           epred = triplet => triplet.attr.typeLabel == "Person_knows_Person")
  }

}
