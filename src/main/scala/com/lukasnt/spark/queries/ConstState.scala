package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.Properties

class ConstState(val seqNum: Int = 0) extends Serializable {

  private var _iterations: Int       = 0
  private var _currentLength: Int    = 0
  private var _source: Boolean       = false
  private var _intermediate: Boolean = false
  private var _destination: Boolean  = false

  def iterations: Int = _iterations

  def currentLength: Int = _currentLength

  def source: Boolean = _source

  def intermediate: Boolean = _intermediate

  def destination: Boolean = _destination


  override def toString: String = {
    s"QueryState(seqNum=$seqNum, iterations=${_iterations}, currentLength=${_currentLength}, " +
      s"intermediate=${_intermediate}, source=${_source}, destination=${_destination}"
  }

}

object ConstState {

  def builder() = new ConstStateBuilder()

  class ConstStateBuilder {

    private val queryState: ConstState = new ConstState()

    def build(): ConstState = {
      queryState
    }

    def fromState(state: ConstState): ConstStateBuilder = {
      queryState._iterations = state._iterations
      queryState._currentLength = state._currentLength
      queryState._source = state._source
      queryState._intermediate = state._intermediate
      queryState._destination = state._destination
      this
    }

    def applySourceTest(testFunc: Properties => Boolean, nodeProperties: Properties): ConstStateBuilder = {
      queryState._source = testFunc(nodeProperties)
      this
    }

    def applyIntermediateTest(testFunc: Properties => Boolean, edgeProperties: Properties): ConstStateBuilder = {
      queryState._intermediate = testFunc(edgeProperties)
      this
    }

    def applyDestinationTest(testFunc: Properties => Boolean, nodeProperties: Properties): ConstStateBuilder = {
      queryState._destination = testFunc(nodeProperties)
      this
    }

    def incIterations(): ConstStateBuilder = {
      queryState._iterations += 1
      this
    }

    def setCurrentLength(length: Int): ConstStateBuilder = {
      queryState._currentLength = length
      this
    }

  }

}
