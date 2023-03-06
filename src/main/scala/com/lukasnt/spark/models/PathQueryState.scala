package com.lukasnt.spark.models

class PathQueryState {

  val query: TemporalPathQuery = null
  val prev: PathQueryState     = null
  val next: PathQueryState     = null
  val costComputed: Float      = 0.0f
  val testSuccess: Boolean     = false
  val currentLength: Int       = 0
  val completed: Boolean       = false

  def this(query: TemporalPathQuery) {
    this()
  }

}
