package com.lukasnt.spark.experiments

import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

case class QueryExecutionResult(
    runNumber: Int,
    queryName: String,
    query: ParameterQuery,
    graphName: String,
    graphSize: String,
    sparkExecutorInstances: Int,
    executorName: String,
    partitionStrategy: String,
    queryResult: QueryResult,
    experimentExecutionInfo: ExperimentExecutionInfo,
    experimentMaxMemoryInfo: ExperimentMemoryInfo
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
    println(s"$executorName execution time: " + experimentExecutionInfo.totalExecutionTime + " ms")
    println(s"${other.executorName} execution time: ${other.experimentExecutionInfo.totalExecutionTime} ms")
    println(
      s"$executorName was ${other.experimentExecutionInfo.totalExecutionTime / experimentExecutionInfo.totalExecutionTime - 1} times faster than ${other.executorName}"
    )
    println("-------------------------------------")
    println("Query results difference:")
    queryResult.printDiff(other = other.queryResult)
    println("=====================================")
  }

  def infoResultsAsDataFrame(): Row = {
    Row(
      runNumber,
      queryName,
      query.temporalPathType.toString,
      query.minLength,
      query.maxLength,
      query.topK,
      graphName,
      graphSize,
      sparkExecutorInstances,
      executorName,
      partitionStrategy,
      experimentExecutionInfo.subgraphPhaseTime,
      experimentExecutionInfo.weightMapPhaseTime,
      experimentExecutionInfo.pregelPhaseTime,
      experimentExecutionInfo.pathConstructionPhaseTime,
      experimentExecutionInfo.totalExecutionTime,
      experimentMaxMemoryInfo.driverTotalMemory,
      experimentMaxMemoryInfo.driverMemoryFree,
      experimentMaxMemoryInfo.driverMemoryUsed,
      experimentMaxMemoryInfo.totalSparkExecutorMemoryAllocated,
      experimentMaxMemoryInfo.totalSparkExecutorMemoryFree,
      experimentMaxMemoryInfo.totalSparkExecutorMemoryUsed,
      experimentMaxMemoryInfo.totalRDDMemorySize,
      experimentMaxMemoryInfo.totalRDDDiskSize,
      experimentMaxMemoryInfo.totalMemoryUsed,
      experimentMaxMemoryInfo.driverMemoryUsedMB,
      experimentMaxMemoryInfo.totalSparkExecutorMemoryUsedMB,
      experimentMaxMemoryInfo.totalRDDMemorySizeMB,
      experimentMaxMemoryInfo.totalRDDDiskSizeMB,
      experimentMaxMemoryInfo.totalMemoryUsedMB
    )
  }

}

object QueryExecutionResult {

  def infoResultsAsDataFrameSchema(): StructType = {
    StructType(
      List(
        StructField("runNumber", IntegerType, nullable = false),
        StructField("queryName", StringType, nullable = false),
        StructField("pathType", StringType, nullable = false),
        StructField("minLength", IntegerType, nullable = false),
        StructField("maxLength", IntegerType, nullable = false),
        StructField("topK", IntegerType, nullable = false),
        StructField("graphName", StringType, nullable = false),
        StructField("graphSize", StringType, nullable = false),
        StructField("sparkExecutorInstances", IntegerType, nullable = false),
        StructField("executorName", StringType, nullable = false),
        StructField("partitionStrategy", StringType, nullable = false),
        StructField("subgraphPhaseTime", LongType, nullable = false),
        StructField("weightMapPhaseTime", LongType, nullable = false),
        StructField("pregelPhaseTime", LongType, nullable = false),
        StructField("pathConstructionPhaseTime", LongType, nullable = false),
        StructField("totalExecutionTime", LongType, nullable = false),
        StructField("driverTotalMemory", LongType, nullable = false),
        StructField("driverMemoryFree", LongType, nullable = false),
        StructField("driverMemoryUsed", LongType, nullable = false),
        StructField("totalSparkExecutorMemoryAllocated", LongType, nullable = false),
        StructField("totalSparkExecutorMemoryFree", LongType, nullable = false),
        StructField("totalSparkExecutorMemoryUsed", LongType, nullable = false),
        StructField("totalRDDMemorySize", LongType, nullable = false),
        StructField("totalRDDDiskSize", LongType, nullable = false),
        StructField("totalMemoryUsed", LongType, nullable = false),
        StructField("driverMemoryUsedMB", IntegerType, nullable = false),
        StructField("totalSparkExecutorMemoryUsedMB", IntegerType, nullable = false),
        StructField("totalRDDMemorySizeMB", IntegerType, nullable = false),
        StructField("totalRDDDiskSizeMB", IntegerType, nullable = false),
        StructField("totalMemoryUsedMB", IntegerType, nullable = false)
      )
    )
  }

}
