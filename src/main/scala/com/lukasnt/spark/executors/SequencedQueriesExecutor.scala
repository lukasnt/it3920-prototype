package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{QueryResult, SequencedQueries}

object SequencedQueriesExecutor {

  def execute(sequencedQueries: SequencedQueries, temporalGraph: TemporalGraph): QueryResult = {
    val subgraphs     = SubgraphFilterExecutor.executeSubgraphFilter(sequencedQueries, temporalGraph)
    val pregelGraph   = ConstPregelRunner.run(sequencedQueries, subgraphs)
    val sequencePaths = ConstJoinExecutor.createConstPaths(sequencedQueries, pregelGraph)
    val paths         = ConstJoinExecutor.joinSequence(sequencedQueries, sequencePaths)
    new QueryResult(temporalGraph, paths.collect().toList)
  }

}
