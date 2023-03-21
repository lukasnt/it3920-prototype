package com.lukasnt.spark.queries

class ConstStateMessages(val queryStates: List[ConstState]) extends Serializable {

  def getMinCostBy(seqNum: Int): Float = {
    val seqFiltered = queryStates.filter(_.seqNum == seqNum)
    if (seqFiltered.isEmpty) Float.MaxValue else seqFiltered.map(_.pathCost).min
  }

  def getMinCostBy(seqNum: Int, length: Int): Float = {
    val seqLenFiltered = queryStates.filter(q => q.seqNum == seqNum && q.currentLength == length)
    if (seqLenFiltered.isEmpty) Float.MaxValue else seqLenFiltered.map(_.pathCost).min
  }

  def merge(other: ConstStateMessages): ConstStateMessages = {
    new ConstStateMessages(queryStates ++ other.queryStates)
  }

}
