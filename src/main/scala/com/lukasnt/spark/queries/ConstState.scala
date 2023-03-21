package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.{Interval, Properties}
import org.apache.spark.graphx.EdgeTriplet

class ConstState(val seqNum: Int = 0) extends Serializable {

  var superstep: Int       = 0
  var pathCost: Float      = Float.MaxValue
  var nodeCost: Float      = 0.0f
  var testSuccess: Boolean = false
  var currentLength: Int   = 0
  var completed: Boolean   = false

  def this(query: ConstQuery) {
    this()
  }

  override def toString: String = {
    s"QueryState(superstep=$superstep, seqNum=$seqNum, pathCost=$pathCost, nodeCost=$nodeCost, " +
      s"testSuccess=$testSuccess, currentLength=$currentLength, completed=$completed)"
  }

  /**
    * Combine all the fields of the state into a single hash code
    */
  override def hashCode(): Int = {
    val state = Seq(superstep, seqNum, pathCost, nodeCost, testSuccess, currentLength, completed)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
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
      queryState = state
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

    def applyNodeTest(nodeProperties: Properties, testFunc: Properties => Boolean): ConstStateBuilder = {
      queryState.testSuccess = testFunc(nodeProperties)
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
        queryState.testSuccess = true
        //queryState.currentLength = srcState(queryState.seqNum).currentLength + 1
      } else {
        queryState.testSuccess = false
      }

      //queryState.pathCost = 1005.0f
      this
    }

  }

}
