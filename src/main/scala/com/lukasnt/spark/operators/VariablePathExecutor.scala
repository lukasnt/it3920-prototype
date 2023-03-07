package com.lukasnt.spark.operators

import com.lukasnt.spark.models.{PathQueryState, TemporalProperties, VariablePathQuery}

import java.time.ZonedDateTime

object VariablePathExecutor {
  def execute(variablePathQuery: VariablePathQuery,
              state: PathQueryState,
              node: TemporalProperties[ZonedDateTime]): PathQueryState = ???
}
