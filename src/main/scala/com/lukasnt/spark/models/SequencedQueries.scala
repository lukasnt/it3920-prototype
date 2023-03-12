package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

class SequencedQueries(val sequence: List[(PathQuery, QueryAggFunc)] = List()) {

  def concatPathQuery(constantPathQuery: PathQuery, aggFunc: QueryAggFunc): SequencedQueries = {
    new SequencedQueries(sequence :+ (constantPathQuery, aggFunc))
  }

  def createInitStates(): List[QueryState] = {
    sequence.zipWithIndex.map(q => new QueryState(q._2))
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
      case q: ConstQuery     => q
      case q: VariableQuery  => q.constQuery
      case q: ArbitraryQuery => q.constQuery
      case _                 => new ConstQuery()
    }
  }

}
