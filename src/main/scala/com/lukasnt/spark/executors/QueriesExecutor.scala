package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult, SequencedQueries}

object QueriesExecutor {

  def execute(sequencedQueries: SequencedQueries, temporalGraph: TemporalGraph): QueryResult = {
    val subgraph    = SequenceSubgraph(temporalGraph, sequencedQueries)
    val pregelGraph = ConstPregel(subgraph, sequencedQueries)
    val paths       = ConstPathsConstruction(pregelGraph, sequencedQueries)
    new QueryResult(temporalGraph, paths.collect().toList)
  }

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult = {
    val subgraph      = ParameterSubgraph(temporalGraph, parameterQuery)
    val weightedGraph = ParameterWeightMap(subgraph, parameterQuery)
    val pregelGraph   = ParameterPregel(weightedGraph, parameterQuery)
    new QueryResult(temporalGraph, List())
  }

}
