package com.lukasnt.spark.util

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types.Interval
import com.lukasnt.spark.util.IntervalStates.{IntervalEntry, IntervalTable}

class IntervalStates extends Serializable {

  val intervalTables: List[IntervalTable] = List()

  def updateWithTable(intervalTable: IntervalTable, topK: Int): IntervalStates = {
    if (intervalTables.exists(_.interval == intervalTable.interval)) {
      this.mergeStates(IntervalStates(List(intervalTable)), topK)
    } else {
      IntervalStates(intervalTables :+ intervalTable)
    }
  }

  def mergeStates(otherStates: IntervalStates, topK: Int): IntervalStates = {
    val newIntervalTables = intervalTables ++ otherStates.intervalTables
    IntervalStates(
      newIntervalTables
        .groupBy(intervalTable => intervalTable.interval)
        .map {
          case (interval, tables) =>
            IntervalTable(
              interval,
              tables.map(_.table).reduce((a, b) => a.mergeWithTable(b, topK))
            )
        }
        .toList
    )
  }

  def intervalFilteredStates(filterFunction: (Interval, Interval) => Boolean, interval: Interval): IntervalStates = {
    IntervalStates(intervalTables.filter(intervalTable => filterFunction(intervalTable.interval, interval)))
  }

  def lengthFilteredStates(length: Int): IntervalStates = {
    IntervalStates(
      intervalTables
        .map(intervalTable =>
          IntervalTable(intervalTable.interval, intervalTable.table.filterByLength(length, topK = -1)))
        .filter(_.table.entries.nonEmpty)
    )
  }

  def flattenEntries(topK: Int): List[IntervalEntry] = {
    intervalTables
      .flatMap(
        intervalTable =>
          intervalTable.table.entries
            .map(entry => IntervalEntry(intervalTable.interval, entry))
            .sortBy(_.entry.weight)
            .take(topK))
      .sortBy(_.entry.weight)
      .take(topK)
  }

  def flattenEntries: List[IntervalEntry] = {
    intervalTables.flatMap(intervalTable =>
      intervalTable.table.entries.map(entry => IntervalEntry(intervalTable.interval, entry)))
  }

  def flushedTableStates(topK: Int): IntervalStates = {
    IntervalStates(intervalTables.map(intervalTable =>
      IntervalTable(intervalTable.interval, intervalTable.table.flushActiveEntries(topK))))
  }

  def firstTable: LengthWeightTable = {
    if (intervalTables.nonEmpty) intervalTables.head.table else LengthWeightTable(List(), List(), 0)
  }

  def firstInterval: Interval = {
    if (intervalTables.nonEmpty) intervalTables.head.interval else TemporalInterval()
  }

  def currentLength: Int = {
    if (intervalTables.nonEmpty) intervalTables.map(_.table.currentLength).max else 0
  }

  override def equals(other: Any): Boolean = {
    other match {
      case otherStates: IntervalStates =>
        this.intervalTables.sortBy(_.interval.startTime.toInstant) ==
          otherStates.intervalTables.sortBy(_.interval.startTime.toInstant)
      case _ => false
    }
  }

  override def toString: String = {
    s"IntervalStates[\n${intervalTables.mkString("\n")}\n]"
  }

}

object IntervalStates {

  def apply(data: List[IntervalTable]): IntervalStates = new IntervalStates {
    override val intervalTables: List[IntervalTable] = data
  }

  def apply(): IntervalStates = new IntervalStates

  case class IntervalTable(interval: Interval, table: LengthWeightTable) {
    override def toString: String = {
      s"IntervalTable($interval, $table)"
    }
  }

  case class IntervalEntry(interval: Interval, entry: LengthWeightTable.Entry) {
    override def toString: String = {
      s"IntervalEntry($interval, $entry)"
    }
  }

}
