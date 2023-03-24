package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types.Interval

class IntervalMessage extends Serializable {

  val interval: Interval                   = TemporalInterval()
  val length: Int                          = 0
  val lengthWeightTable: LengthWeightTable = LengthWeightTable(List())

  override def toString: String = {
    s"IntervalMessage(interval=$interval, length=$length, lengthWeightTable=$lengthWeightTable)"
  }

}

object IntervalMessage {

  def apply(interval: Interval, length: Int, lengthWeightTable: LengthWeightTable): IntervalMessage = {
    val newInterval          = interval
    val newLength            = length
    val newLengthWeightTable = lengthWeightTable
    new IntervalMessage {
      override val interval: Interval                   = newInterval
      override val length: Int                          = newLength
      override val lengthWeightTable: LengthWeightTable = newLengthWeightTable
    }
  }

}
