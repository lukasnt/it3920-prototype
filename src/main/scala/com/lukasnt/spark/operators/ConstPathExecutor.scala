package com.lukasnt.spark.operators

import com.lukasnt.spark.models.{ConstPathQuery, PathQueryState, TemporalProperties}

import java.time.ZonedDateTime

object ConstPathExecutor {

  def execute(query: ConstPathQuery,
              currentState: PathQueryState,
              nodeProperties: TemporalProperties[ZonedDateTime]): PathQueryState = {
    val newState = new PathQueryState(currentState.seqNum, null)
    newState.costComputed = query.costFunc(nodeProperties)
    newState.testSuccess = query.testFunc(nodeProperties)
    newState.currentLength = 1
    newState.completed = newState.testSuccess
    newState
  }

}
