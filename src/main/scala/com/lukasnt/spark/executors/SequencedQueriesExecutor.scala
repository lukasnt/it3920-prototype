package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.models.{QueryResult, SequencedQueries, UnweightedQueries}

object SequencedQueriesExecutor {

  def execute(sequencedQueries: UnweightedQueries, temporalGraph: TemporalGraph): QueryResult = {
    val subgraphs     = SubgraphFilterExecutor.executeSubgraphFilter(sequencedQueries, temporalGraph)
    val pregelGraph   = UnweightedPregelRunner.run(sequencedQueries, subgraphs)
    val sequencePaths = PathsJoinExecutor.createConstPaths(sequencedQueries, pregelGraph)
    val paths         = PathsJoinExecutor.joinSequence(sequencedQueries, sequencePaths)
    new QueryResult(temporalGraph, paths.collect().toList)
  }

  def execute(sequencedQueries: SequencedQueries, temporalGraph: TemporalGraph): QueryResult = {
    val subgraphs = SubgraphFilterExecutor.executeSubgraphFilter(sequencedQueries, temporalGraph)
    ???
  }
}
