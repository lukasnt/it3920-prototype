package com.lukasnt.spark.executors
import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}
import org.apache.spark.graphx.PartitionStrategy

trait ParameterQueryExecutor {

  def execute(parameterQuery: ParameterQuery,
              temporalGraph: TemporalGraph,
              partitionStrategy: PartitionStrategy = PartitionStrategy.RandomVertexCut): QueryResult

}

object ParameterQueryExecutor {

  def getByName(
      name: String
  ): ParameterQueryExecutor =
    name match {
      case "serial" => SerialQueryExecutor()
      case "spark"  => SparkQueryExecutor()
      case _        => SparkQueryExecutor()
    }

}
