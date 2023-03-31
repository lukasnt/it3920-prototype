package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types.Interval
import com.lukasnt.spark.queries.IntervalStates.IntervalTable

class IntervalStates extends Serializable {

  val intervalTables: List[IntervalTable] = List()

  def updateWithTable(intervalTable: IntervalTable): IntervalStates = {
    // TODO: Need to handle overlapping intervals and split them into multiple entries according to our interval logic
    val newIntervalData = intervalTables :+ intervalTable
    // Right now we will just replace the current interval entry with the new one, so that we can focus on one interval at a time
    IntervalStates(List(intervalTable))
  }

  def updateWithTables(newIntervalTables: List[IntervalTable]): IntervalStates = {
    IntervalStates(intervalTables ++ newIntervalTables)
  }

  def firstTable: LengthWeightTable = {
    if (intervalTables.nonEmpty) intervalTables.head.table else LengthWeightTable(List(), List(), 0)
  }

  def firstInterval: Interval = {
    if (intervalTables.nonEmpty) intervalTables.head.interval else TemporalInterval()
  }

  def lengthFilteredTable(minLength: Int, maxLength: Int, topK: Int): LengthWeightTable = {
    intervalTables
      .map(_.table.filterByLengthRange(minLength, maxLength, topK))
      .reduce(_.mergeWithTable(_, topK))
  }

  def lengthFilteredTable(length: Int, topK: Int): LengthWeightTable = {
    intervalTables
      .map(_.table.filterByLength(length, topK))
      .reduce(_.mergeWithTable(_, topK))
  }

  def intervalFilteredTable(interval: Interval, topK: Int): LengthWeightTable = {
    intervalTables
      .filter(intervalTable => intervalTable.interval.during(interval) || intervalTable.interval.equals(interval))
      .map(_.table)
      .reduce(_.mergeWithTable(_, topK))
  }

  def intervalFilteredTable(filterFunction: (Interval, Interval) => Boolean,
                            interval: Interval,
                            topK: Int): LengthWeightTable = {
    intervalTables
      .filter(intervalTable => filterFunction(intervalTable.interval, interval))
      .map(_.table)
      .reduce(_.mergeWithTable(_, topK))
  }

  override def toString: String = {
    s"[${intervalTables.mkString(", ")}]"
  }

}

object IntervalStates {

  def apply(data: List[IntervalTable]): IntervalStates = new IntervalStates {
    override val intervalTables: List[IntervalTable] = data
  }

  def apply(): IntervalStates = new IntervalStates

  case class IntervalTable(interval: Interval, table: LengthWeightTable) {
    override def toString: String = {
      s"IntervalEntry(${interval.toString}, ${table.toString})"
    }
  }
}
