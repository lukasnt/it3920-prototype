package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.{Interval, Properties}
import org.apache.spark.graphx.EdgeTriplet

class ConstState(val seqNum: Int = 0) extends Serializable {

  var superstep: Int        = 0
  var pathCost: Float       = Float.MaxValue
  var nodeCost: Float       = 0.0f
  var source: Boolean       = false
  var intermediate: Boolean = false
  var destination: Boolean  = false
  var currentLength: Int    = 0

  override def toString: String = {
    s"QueryState(seqNum=$seqNum, superstep=$superstep, pathCost=$pathCost, nodeCost=$nodeCost, " +
      s"intermediate=$intermediate, source=$source, destination=$destination, currentLength=$currentLength"
  }

}

object ConstState {

  def builder() = new ConstStateBuilder()

  class ConstStateBuilder {

    var queryState: ConstState = new ConstState()

    def build(): ConstState = {
      queryState
    }

    def fromState(state: ConstState): ConstStateBuilder = {
      queryState.superstep = state.superstep
      queryState.pathCost = state.pathCost
      queryState.nodeCost = state.nodeCost
      queryState.source = state.source
      queryState.intermediate = state.intermediate
      queryState.destination = state.destination
      queryState.currentLength = state.currentLength
      this
    }

    def withSeqNum(seqNum: Int): ConstStateBuilder = {
      queryState = new ConstState(seqNum)
      this
    }

    def withInitPathCost(pathCost: Float): ConstStateBuilder = {
      queryState.pathCost = pathCost
      this
    }

    def applySourceTest(nodeProperties: Properties, testFunc: Properties => Boolean): ConstStateBuilder = {
      queryState.source = testFunc(nodeProperties)
      this
    }

    def applyIntermediateTest(edgeProperties: Properties, testFunc: Properties => Boolean): ConstStateBuilder = {
      queryState.intermediate = testFunc(edgeProperties)
      this
    }

    def applyDestinationTest(nodeProperties: Properties, testFunc: Properties => Boolean): ConstStateBuilder = {
      queryState.destination = testFunc(nodeProperties)
      this
    }

    def applyNodeCost(nodeProperties: Properties, costFunc: Properties => Float): ConstStateBuilder = {
      queryState.nodeCost = costFunc(nodeProperties)
      this
    }

    def applyPathCostUpdate(pathCost: Float): ConstStateBuilder = {
      queryState.pathCost = Math.min(queryState.pathCost, pathCost)
      this
    }

    def incSuperstep(): ConstStateBuilder = {
      queryState.superstep += 1
      this
    }

    def incCurrentLength(): ConstStateBuilder = {
      queryState.currentLength += 1
      this
    }

    def applyWeightedPregelTriplet(edgeTriplet: EdgeTriplet[(Properties, List[ConstState]), Properties],
                                   aggTest: (Properties, Properties, Properties) => Boolean,
                                   aggIntervalTest: (Interval, Interval) => Boolean,
                                   aggCost: (Float, Properties) => Float): ConstStateBuilder = {

      // Extract all the properties and functions from the edge triplet
      val (srcNode, srcState)   = edgeTriplet.srcAttr
      val (dstNode, dstState)   = edgeTriplet.dstAttr
      val edgeProperties        = edgeTriplet.attr
      val aggTestResult         = aggTest(srcNode, dstNode, edgeProperties)
      val aggIntervalTestResult = aggIntervalTest(srcNode.interval, edgeProperties.interval)

      if (aggTestResult && aggIntervalTestResult) {
        val aggCostResult = aggCost(queryState.pathCost, edgeProperties)
        queryState.pathCost = aggCostResult
        queryState.intermediate = true
        //queryState.currentLength = srcState(queryState.seqNum).currentLength + 1
      } else {
        queryState.intermediate = false
      }

      //queryState.pathCost = 1005.0f
      this
    }

  }

}
