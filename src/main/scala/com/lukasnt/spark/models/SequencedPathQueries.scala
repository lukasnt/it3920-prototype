package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

import scala.collection.mutable

class SequencedPathQueries(val pathQuerySequence: List[(PathQuery, PathAggFunc)] = List()) {

  def concatPathQuery(constantPathQuery: PathQuery, aggFunc: PathAggFunc): SequencedPathQueries = {
    new SequencedPathQueries(pathQuerySequence :+ (constantPathQuery, aggFunc))
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

  def getQueryBySeqNum(seqNum: Int): PathQuery = {
    pathQuerySequence(seqNum)._1
  }

  def getAggFuncBySeqNum(seqNum: Int): PathAggFunc = {
    pathQuerySequence(seqNum)._2
  }

}
