package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{Interval, Properties}
import com.lukasnt.spark.models.{ConstQuery, QueryState}
import org.apache.spark.graphx.EdgeTriplet

object QueryStateMapper {

  def mapConstQuery(currentState: QueryState, nodeProperties: Properties, query: ConstQuery): QueryState = {
    val newState = new QueryState(currentState.seqNum)
    newState.nodeCost = query.nodeCost(nodeProperties)
    newState.testSuccess = query.nodeTest(nodeProperties)
    newState.currentLength = 1
    newState.completed = true
    newState
  }

  def mapNodeTest(currentState: QueryState, nodeProperties: Properties, nodeTest: Properties => Boolean): QueryState = {
    val newState = new QueryState(currentState.seqNum)
    newState.testSuccess = nodeTest(nodeProperties)
    newState.completed = true
    newState
  }

  def mapNodeCost(currentState: QueryState, nodeProperties: Properties, nodeCost: Properties => Float): QueryState = {
    val newState = new QueryState(currentState.seqNum)
    newState.nodeCost = nodeCost(nodeProperties)
    newState.completed = true
    newState
  }

  def mapWeightedPregelTriplet(queryState: QueryState,
                               edgeTriplet: EdgeTriplet[(Properties, List[QueryState]), Properties],
                               aggTest: (Properties, Properties, Properties) => Boolean,
                               aggIntervalTest: (Interval, Interval, Interval) => Boolean,
                               aggCost: (Float, Float, Properties) => Float): QueryState = {
    val (srcNode, srcState) = edgeTriplet.srcAttr
    val (dstNode, dstState) = edgeTriplet.dstAttr
    val edgeProperties      = edgeTriplet.attr

    val aggTestResult         = aggTest(srcNode, edgeProperties, dstNode)
    val aggIntervalTestResult = aggIntervalTest(srcNode.interval, dstNode.interval, edgeProperties.interval)
    if (aggTestResult && aggIntervalTestResult) {
      val aggCostResult =
        aggCost(srcState(queryState.seqNum).nodeCost, dstState(queryState.seqNum).nodeCost, edgeProperties)
      val newState = new QueryState(queryState.seqNum)
      newState.nodeCost = aggCostResult
      newState.testSuccess = true
      newState.currentLength = srcState(queryState.seqNum).currentLength + 1
      newState.completed = true

      newState
    } else {
      new QueryState(queryState.seqNum)
    }

  }

}
