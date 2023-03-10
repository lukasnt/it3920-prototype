package com.lukasnt.spark.models

class QueryState(val seqNum: Int = 0, val next: QueryState = null) extends Serializable {

  var costComputed: Float  = 0.0f
  var testSuccess: Boolean = false
  var currentLength: Int   = 0
  var completed: Boolean   = false

  def this(query: ConstQuery) {
    this()
  }

  override def toString: String = {
    s"QueryState(seqNum=$seqNum, costComputed=$costComputed, testSuccess=$testSuccess, " +
      s"currentLength=$currentLength, completed=$completed)"
  }
}
