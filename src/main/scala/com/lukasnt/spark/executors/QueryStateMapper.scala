package com.lukasnt.spark.executors

import com.lukasnt.spark.models.{ConstQuery, QueryState, TemporalProperties}

import java.time.ZonedDateTime

object QueryStateMapper {

  def mapConstQuery(query: ConstQuery,
                    currentState: QueryState,
                    nodeProperties: TemporalProperties[ZonedDateTime]): QueryState = {
    val newState = new QueryState(currentState.seqNum, null)
    newState.costComputed = query.costFunc(nodeProperties)
    newState.testSuccess = query.testFunc(nodeProperties)
    newState.currentLength = 1
    newState.completed = newState.testSuccess
    newState
  }

}
