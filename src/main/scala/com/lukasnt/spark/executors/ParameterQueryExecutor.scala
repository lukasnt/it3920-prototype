package com.lukasnt.spark.executors
import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}
import org.apache.spark.graphx.PartitionStrategy

trait ParameterQueryExecutor {

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult

}

object ParameterQueryExecutor {

  def getByName(
      name: String,
      partitionStrategy: PartitionStrategy = PartitionStrategy.RandomVertexCut
  ): ParameterQueryExecutor =
    name match {
      case "serial" => SerialQueryExecutor()
      case "spark"  => SparkQueryExecutor(partitionStrategy)
      case _        => SparkQueryExecutor()
    }

}
