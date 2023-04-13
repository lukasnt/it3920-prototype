package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}

trait ParameterQueryExecutor {

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult

}
