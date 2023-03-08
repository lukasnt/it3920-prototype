package com.lukasnt.spark.models

class PathQueryState(val seqNum: Int = 0, val next: PathQueryState = null) extends Serializable {

  var costComputed: Float  = 0.0f
  var testSuccess: Boolean = false
  var currentLength: Int   = 0
  var completed: Boolean   = false

  def this(query: ConstPathQuery) {
    this()
  }

  override def toString: String = {
    s"PathQueryState(seqNum=$seqNum, costComputed=$costComputed, " +
      s"testSuccess=$testSuccess, currentLength=$currentLength, completed=$completed)"
  }
}
