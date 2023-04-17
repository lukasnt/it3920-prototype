package com.lukasnt.spark.experiments

import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}
import org.apache.spark.sql.Row

case class QueryExecutionResult(
    query: ParameterQuery,
    graphName: String,
    executorName: String,
    queryResult: QueryResult,
    executionTime: Long
) {

  def printComparison(other: QueryExecutionResult): Unit = {
    println("=====================================")
    println("Query:")
    println(query)
    println("-------------------------------------")
    println("Comparison:")
    println(s"Graph Loaders: $graphName vs ${other.graphName}")
    println(s"Executors: $executorName vs ${other.executorName}")
    println("-------------------------------------")
    println("Execution times:")
    println(s"$executorName execution time: " + executionTime + " ms")
    println(s"${other.executorName} execution time: ${other.executionTime} ms")
    println(s"$executorName was ${other.executionTime / executionTime - 1} times faster than ${other.executorName}")
    println("-------------------------------------")
    println("Query results difference:")
    queryResult.printDiff(other = other.queryResult)
    println("=====================================")
  }

}
