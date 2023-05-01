package com.lukasnt.spark.util

import com.lukasnt.spark.models.Types.Properties

class QueryState(val seqNum: Int = 0) extends Serializable {

  private var _iterations: Int       = 0
  private var _source: Boolean       = false
  private var _intermediate: Boolean = false
  private var _destination: Boolean  = false

  def iterations: Int = _iterations

  def source: Boolean = _source

  def intermediate: Boolean = _intermediate

  def destination: Boolean = _destination

  override def toString: String = {
    s"QueryState(seqNum=$seqNum, iterations=${_iterations}, intermediate=${_intermediate}, " +
      s"source=${_source}, destination=${_destination}"
  }

}

object QueryState {

  def builder() = new QueryStateBuilder()

  class QueryStateBuilder {

    private val queryState: QueryState = new QueryState()

    def build(): QueryState = {
      queryState
    }

    def fromState(state: QueryState): QueryStateBuilder = {
      queryState._iterations = state._iterations
      queryState._source = state._source
      queryState._intermediate = state._intermediate
      queryState._destination = state._destination
      this
    }

    def applySourceTest(testFunc: Properties => Boolean, nodeProperties: Properties): QueryStateBuilder = {
      queryState._source = testFunc(nodeProperties)
      this
    }

    def applyIntermediateTest(testFunc: Properties => Boolean, edgeProperties: Properties): QueryStateBuilder = {
      queryState._intermediate = testFunc(edgeProperties)
      this
    }

    def applyDestinationTest(testFunc: Properties => Boolean, nodeProperties: Properties): QueryStateBuilder = {
      queryState._destination = testFunc(nodeProperties)
      this
    }

    def incIterations(): QueryStateBuilder = {
      queryState._iterations += 1
      this
    }

  }

}
