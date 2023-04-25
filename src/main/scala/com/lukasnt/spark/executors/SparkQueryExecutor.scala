package com.lukasnt.spark.executors

import com.lukasnt.spark.experiments.Experiment
import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}

class SparkQueryExecutor extends ParameterQueryExecutor {

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult = {
    val totalStartTime = System.currentTimeMillis()

    println("Starting subgraph phase...")
    val subgraphStartTime = System.currentTimeMillis()
    val subgraph          = ParameterSubgraph(temporalGraph, parameterQuery)
    val subgraphTime      = System.currentTimeMillis() - subgraphStartTime
    Experiment.measureCurrentExecutionMemory()
    temporalGraph.unpersist()
    println(s"Subgraph phase finished in $subgraphTime ms")

    println("Starting weight-map phase...")
    val weightMapStartTime = System.currentTimeMillis()
    val weightedGraph      = ParameterWeightMap(subgraph, parameterQuery)
    val weightMapTime      = System.currentTimeMillis() - weightMapStartTime
    Experiment.measureCurrentExecutionMemory()
    println(s"Weight-map phase finished in $weightMapTime ms")

    println("Starting pregel-computation phase...")
    val pregelStartTime = System.currentTimeMillis()
    val pregelGraph     = ParameterPregel(weightedGraph, parameterQuery)
    val pregelTime      = System.currentTimeMillis() - pregelStartTime
    Experiment.measureCurrentExecutionMemory()
    weightedGraph.unpersist()
    println(s"Pregel-computation phase finished in $pregelTime ms")

    println("Starting path-construction phase...")
    val pathConstructionStartTime = System.currentTimeMillis()
    val pathTable                 = ParameterPathsConstruction(pregelGraph, parameterQuery)
    val pathConstructionPhaseTime = System.currentTimeMillis() - pathConstructionStartTime
    Experiment.measureCurrentExecutionMemory()
    println(s"Path-construction phase finished in $pathConstructionPhaseTime ms")

    val totalExecutionTime = System.currentTimeMillis() - totalStartTime
    println(s"Total execution time: $totalExecutionTime ms")

    Experiment.measureExecutionTime(
      subgraphPhaseTime = subgraphTime,
      weightMapPhaseTime = weightMapTime,
      pregelPhaseTime = pregelTime,
      pathConstructionPhaseTime = pathConstructionPhaseTime,
      totalExecutionTime = totalExecutionTime
    )

    new QueryResult(subgraph, pathTable)
  }

}

object SparkQueryExecutor {

  def apply(): SparkQueryExecutor = new SparkQueryExecutor()

}
