package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types.Interval
import com.lukasnt.spark.queries.IntervalsState.Entry

class IntervalsState extends Serializable {

  val intervalData: List[Entry] = List()

  def updateWithEntry(entry: Entry): IntervalsState = {
    // TODO: Need to handle overlapping intervals and split them into multiple entries according to our interval logic
    val newIntervalData = intervalData :+ entry
    // Right now we will just replace the current interval entry with the new one, so that we can focus on one interval at a time
    IntervalsState(List(entry))
  }

  def updateWithEntries(entries: List[Entry]): IntervalsState = {
    IntervalsState(intervalData ++ entries)
  }

  def firstTable: LengthWeightTable = {
    if (intervalData.nonEmpty) intervalData.head.lengthWeightTable else LengthWeightTable(List(), List(), 0)
  }

  def firstInterval: Interval = {
    if (intervalData.nonEmpty) intervalData.head.interval else TemporalInterval()
  }

  override def toString: String = {
    s"[${intervalData.mkString(", ")}]"
  }

}

object IntervalsState {

  def apply(data: List[Entry]): IntervalsState = new IntervalsState {
    override val intervalData: List[Entry] = data
  }

  def apply(): IntervalsState = new IntervalsState

  case class Entry(interval: Interval, lengthWeightTable: LengthWeightTable) {
    override def toString: String = {
      s"IntervalEntry(${interval.toString}, ${lengthWeightTable.toString})"
    }
  }
}
