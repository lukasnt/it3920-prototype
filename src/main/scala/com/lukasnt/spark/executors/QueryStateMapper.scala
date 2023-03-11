package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.Properties
import com.lukasnt.spark.models.{ConstQuery, QueryState}

object QueryStateMapper {

  def mapConstQuery(query: ConstQuery, currentState: QueryState, nodeProperties: Properties): QueryState = {
    val newState = new QueryState(currentState.seqNum, null)
    newState.costComputed = query.costFunc(nodeProperties)
    newState.testSuccess = query.testFunc(nodeProperties)
    newState.currentLength = 1
    newState.completed = true
    newState
  }

  def mapNodeTest(nodeTest: Properties => Boolean, currentState: QueryState, nodeProperties: Properties): QueryState = {
    val newState = new QueryState(currentState.seqNum, null)
    newState.testSuccess = nodeTest(nodeProperties)
    newState.completed = true
    newState
  }

}
