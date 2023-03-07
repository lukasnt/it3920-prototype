package com.lukasnt.spark.models

import scala.collection.mutable

class TemporalPathQuery(val pathQuerySequence: List[(ConstPathQuery, PathAggFunc)] = List()) {

  def concatPathQuery(constantPathQuery: ConstPathQuery, aggFunc: PathAggFunc): TemporalPathQuery = {
    new TemporalPathQuery(pathQuerySequence :+ (constantPathQuery, aggFunc))
  }

  def createInitStates(): List[PathQueryState] = {
    // Create the states in reverse order so that the next reference is set correctly
    val states               = mutable.ArrayBuffer.fill(pathQuerySequence.length)(null: PathQueryState)
    var next: PathQueryState = null
    for (seqNum <- pathQuerySequence.indices.reverse) {
      states(seqNum) = new PathQueryState(seqNum, next)
      next = states(seqNum)
    }
    states.toList
  }

  def getQueryBySeqNum(seqNum: Int): ConstPathQuery = {
    pathQuerySequence(seqNum)._1
  }

  def getAggFuncBySeqNum(seqNum: Int): PathAggFunc = {
    pathQuerySequence(seqNum)._2
  }

}
