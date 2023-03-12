package com.lukasnt.spark.models

class QueryStateMessages(val queryStates: List[QueryState]) extends Serializable {

  def getMinCostBy(seqNum: Int): Float = {
    val seqFiltered = queryStates.filter(_.seqNum == seqNum)
    if (seqFiltered.isEmpty) Float.MaxValue else seqFiltered.map(_.pathCost).min
  }

  def getMinCostBy(seqNum: Int, length: Int): Float = {
    val seqLenFiltered = queryStates.filter(q => q.seqNum == seqNum && q.currentLength == length)
    if (seqLenFiltered.isEmpty) Float.MaxValue else seqLenFiltered.map(_.pathCost).min
  }

  def merge(other: QueryStateMessages): QueryStateMessages = {
    new QueryStateMessages(queryStates ++ other.queryStates)
  }

}
