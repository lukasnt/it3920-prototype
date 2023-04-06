package com.lukasnt.spark

import com.lukasnt.spark.examples.SimpleGraphX
import com.lukasnt.spark.io.Loggers

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    Loggers.root.info("Hello World!")
    Loggers.root.debug("Hello World!")
    /*
    val personGraph = SimpleSNBGraphs.personKnowsGraph()
    val currentQuery = SimpleParameterQueries.interactionPaths(
      topK = 25,
      minLength = 3,
      pathType = TemporalPathType.Continuous
    )
    val subgraph      = ParameterSubgraph(personGraph, currentQuery)
    val weightedGraph = ParameterWeightMap(subgraph, currentQuery)
    val pregelGraph   = ParameterPregel(weightedGraph, currentQuery)
    val paths         = ParameterPathsConstruction(pregelGraph, currentQuery)
    paths.foreach(println)
     */
    SimpleGraphX.run()
  }

}
