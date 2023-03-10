package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.models.{SequencedQueries, TemporalPath}

object SequencedQueriesExecutor {

  def execute(sequencedQueries: SequencedQueries,
              temporalGraph: TemporalGraph): List[TemporalPath] = {
    val subgraphs = SubgraphFilterExecutor.executeSubgraphFilter(sequencedQueries, temporalGraph)
    ???
  }
}
