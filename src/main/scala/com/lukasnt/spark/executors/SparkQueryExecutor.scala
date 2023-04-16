package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}

class SparkQueryExecutor extends ParameterQueryExecutor {

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult = {
    println("Starting subgraph phase...")
    val subgraphStartTime = System.currentTimeMillis()
    val subgraph          = ParameterSubgraph(temporalGraph, parameterQuery)
    temporalGraph.unpersist()
    println(s"Subgraph phase finished in ${System.currentTimeMillis() - subgraphStartTime} ms")

    println("Starting weight-map phase...")
    val weightMapStartTime = System.currentTimeMillis()
    val weightedGraph      = ParameterWeightMap(subgraph, parameterQuery)
    println(s"Weight-map phase finished in ${System.currentTimeMillis() - weightMapStartTime} ms")


    println("Starting pregel-computation phase...")
    val pregelStartTime = System.currentTimeMillis()
    val pregelGraph     = ParameterPregel(weightedGraph, parameterQuery)
    weightedGraph.unpersist()
    println(s"Pregel-computation phase finished in ${System.currentTimeMillis() - pregelStartTime} ms")

    println("Starting path-construction phase...")
    val pathConstructionStartTime = System.currentTimeMillis()
    val pathTable                 = ParameterPathsConstruction(pregelGraph, parameterQuery)
    println(s"Path-construction phase finished in ${System.currentTimeMillis() - pathConstructionStartTime} ms")

    new QueryResult(subgraph, pathTable)
  }

}

object SparkQueryExecutor {

  def apply(): SparkQueryExecutor = new SparkQueryExecutor()

}
