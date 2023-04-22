package com.lukasnt.spark.experiments

import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class QueryExecutionResult(
    runNumber: Int,
    query: ParameterQuery,
    graphName: String,
    sparkExecutorInstances: Int,
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

  def infoResultsAsDataFrame(): Row = {
    Row(
      runNumber,
      query.temporalPathType.toString,
      query.minLength,
      query.maxLength,
      query.topK,
      graphName,
      sparkExecutorInstances,
      executorName,
      executionTime
    )
  }

}

object QueryExecutionResult {

  def infoResultsAsDataFrameSchema(): StructType = {
    StructType(
      List(
        StructField("runNumber", IntegerType, nullable = false),
        StructField("pathType", StringType, nullable = false),
        StructField("minLength", IntegerType, nullable = false),
        StructField("maxLength", IntegerType, nullable = false),
        StructField("topK", IntegerType, nullable = false),
        StructField("graphName", StringType, nullable = false),
        StructField("sparkExecutorInstances", IntegerType, nullable = false),
        StructField("executorName", StringType, nullable = false),
        StructField("executionTime", LongType, nullable = false)
      )
    )
  }

}
