package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

import scala.collection.mutable

class SequencedQueries(val sequence: List[(PathQuery, QueryAggFunc)] = List()) {

  def concatPathQuery(constantPathQuery: PathQuery, aggFunc: QueryAggFunc): SequencedQueries = {
    new SequencedQueries(sequence :+ (constantPathQuery, aggFunc))
  }

  def createInitStates(): List[QueryState] = {
    // Create the states in reverse order so that the next reference is set correctly
    val states               = mutable.ArrayBuffer.fill(sequence.length)(null: QueryState)
    var next: QueryState = null
    for (seqNum <- sequence.indices.reverse) {
      states(seqNum) = new QueryState(seqNum, next)
      next = states(seqNum)
    }
    states.toList
  }

  def getQueryBySeqNum(seqNum: Int): PathQuery = {
    sequence(seqNum)._1
  }

  def getAggFuncBySeqNum(seqNum: Int): QueryAggFunc = {
    sequence(seqNum)._2
  }

}

object SequencedQueries {

  def extractConstQuery(genericQuery: PathQuery): ConstQuery = {
    genericQuery match {
      case q: ConstQuery => q
      case q: VariableQuery => q.constQuery
      case q: ArbitraryQuery => q.constQuery
      case _ => new ConstQuery()
    }
  }
  
}
