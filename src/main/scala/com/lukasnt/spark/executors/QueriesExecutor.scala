package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}

object QueriesExecutor {

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult = {
    val subgraph      = ParameterSubgraph(temporalGraph, parameterQuery)
    val weightedGraph = ParameterWeightMap(subgraph, parameterQuery)
    val pregelGraph   = ParameterPregel(weightedGraph, parameterQuery)
    val pathTable     = ParameterPathsConstruction(pregelGraph, parameterQuery)
    new QueryResult(temporalGraph, pathTable)
  }

}
