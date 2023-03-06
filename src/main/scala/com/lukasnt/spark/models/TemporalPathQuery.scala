package com.lukasnt.spark.models

class TemporalPathQuery {

  val seqNum: Int             = 0
  val prev: TemporalPathQuery = null
  val next: TemporalPathQuery = null
  val cost: Float             = 0.0f
  val test: String            = ""
  val coTest: String          = ""
  val minLength: Int          = 0
  val maxLength: Int          = 0
  val unbounded: Boolean      = false

  def this(test: String) {
    this()
  }

  def this(cost: Float) {
    this()
  }

  def concat(other: TemporalPathQuery): TemporalPathQuery = {
    new TemporalPathQuery()
  }

  def lengthRange(min: Int, max: Int): TemporalPathQuery = {
    new TemporalPathQuery()
  }

  def unboundLengthRange(min: Int): TemporalPathQuery = {
    new TemporalPathQuery()
  }

}
