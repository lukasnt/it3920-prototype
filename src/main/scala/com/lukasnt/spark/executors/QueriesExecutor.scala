package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ConstQueries, ParameterQuery, QueryResult}

object QueriesExecutor {

  def execute(constQueries: ConstQueries, temporalGraph: TemporalGraph): QueryResult = {
    val subgraph    = ConstSubgraph(temporalGraph, constQueries)
    val pregelGraph = ConstPregel(subgraph, constQueries)
    val pathTable   = ConstPathsConstruction(pregelGraph, constQueries)
    new QueryResult(temporalGraph, pathTable)
  }

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult = {
    val subgraph      = ParameterSubgraph(temporalGraph, parameterQuery)
    val weightedGraph = ParameterWeightMap(subgraph, parameterQuery)
    val pregelGraph   = ParameterPregel(weightedGraph, parameterQuery)
    val pathTable     = ParameterPathsConstruction(pregelGraph, parameterQuery)
    new QueryResult(temporalGraph, pathTable)
  }

}
