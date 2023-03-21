package com.lukasnt.spark.queries

class SequencedQueries(val sequence: List[(ConstQuery, QueryAggFunc)] = List()) {

  def concatQuery(query: ConstQuery, aggFunc: QueryAggFunc): SequencedQueries = {
    new SequencedQueries(sequence :+ (query, aggFunc))
  }

  def createInitStates(): List[ConstState] = {
    sequence.zipWithIndex.map(q => new ConstState(q._2))
  }

  def getQueryBySeqNum(seqNum: Int): ConstQuery = {
    sequence(seqNum)._1
  }

  def getAggFuncBySeqNum(seqNum: Int): QueryAggFunc = {
    sequence(seqNum)._2
  }

}
