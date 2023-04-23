package com.lukasnt.spark.util

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types.Interval
import com.lukasnt.spark.util.IntervalStates.IntervalEntry

import scala.collection.immutable.HashMap

class IntervalStates extends Serializable {

  val intervalTables: HashMap[Interval, LengthWeightTable] = HashMap()

  def mergedStates(otherStates: IntervalStates, topK: Int): IntervalStates = {

    /*IntervalStates(intervalTables ++ otherStates.intervalTables.map {
      case (interval: Interval, table: LengthWeightTable) =>
        interval -> {
          if (intervalTables.contains(interval)) {
            intervalTables(interval).mergeWithTable(table, topK)
          } else {
            table
          }
        }
    })
*/

    IntervalStates(intervalTables.merged(otherStates.intervalTables) {
      case ((interval, thisTable), (_, otherTable)) =>
        (interval, thisTable.mergeWithTable(otherTable, topK))
    })

    /*intervalTables ++= otherStates.intervalTables
    IntervalStates(
      intervalTables
        .groupBy(intervalTable => intervalTable.interval)
        .map {
          case (interval, tables) =>
            IntervalTable(
              interval,
              tables.map(_.table).reduce((a, b) => a.mergeWithTable(b, topK))
            )
        }
        .to[ListBuffer]
    )*/
  }

  /*def appendedStates(otherStates: IntervalStates): IntervalStates = {
    intervalTables ++= otherStates.intervalTables
    this
  }*/

  def intervalFilteredStates(filterFunction: (Interval, Interval) => Boolean, interval: Interval): IntervalStates = {
    IntervalStates(intervalTables.filter {
      case (tableInterval, _) => filterFunction(tableInterval, interval)
    })
  }

  def lengthFilteredStates(length: Int): IntervalStates = {
    IntervalStates(
      intervalTables
        .map {
          case (tableInterval, table) => (tableInterval, table.filterByLength(length, topK = -1))
        }
        .filter {
          case (_, table) => table.entries.nonEmpty
        }
    )
  }

  def flattenEntries(topK: Int): List[IntervalEntry] = {
    intervalTables
      .flatMap {
        case (tableInterval, table) =>
          table.entries
            .map(entry => IntervalEntry(tableInterval, entry))
            .sortBy(_.entry.weight)
            .take(topK)
      }
      .toList
      .sortBy(_.entry.weight)
      .take(topK)
  }

  def flattenEntries: List[IntervalEntry] = {
    intervalTables.flatMap {
      case (tableInterval, table) => table.entries.map(entry => IntervalEntry(tableInterval, entry))
    }.toList
  }

  def flushedTableStates(topK: Int): IntervalStates = {
    IntervalStates(
      intervalTables.map {
        case (tableInterval, table) => (tableInterval, table.flushActiveEntries(topK))
      }
    )
  }

  def firstInterval: Interval = {
    if (intervalTables.nonEmpty) intervalTables.head._1 else TemporalInterval()
  }

  def currentLength: Int = {
    if (intervalTables.nonEmpty) intervalTables.map { case (_, table) => table.currentLength }.max else 0
  }

  override def equals(other: Any): Boolean = {
    other match {
      case otherStates: IntervalStates =>
        this.intervalTables.keysIterator.sameElements(otherStates.intervalTables.keysIterator)
      case _ => false
    }
  }

  override def toString: String = {
    s"IntervalStates[\n${intervalTables.mkString("\n")}\n]"
  }

}

object IntervalStates {

  def apply(data: HashMap[Interval, LengthWeightTable]): IntervalStates = new IntervalStates {
    override val intervalTables: HashMap[Interval, LengthWeightTable] = data
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
