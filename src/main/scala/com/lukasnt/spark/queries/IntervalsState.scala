package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.Interval
import com.lukasnt.spark.queries.IntervalsState.Entry
import com.lukasnt.spark.utils.Loggers

class IntervalsState extends Serializable {

  val intervalData: List[Entry] = List()

  def updateWithEntry(entry: Entry): IntervalsState = {
    val newIntervalData = intervalData :+ entry
    //Loggers.default.debug(s"old: $intervalData, new: $newIntervalData, equal: ${intervalData == newIntervalData}")
    IntervalsState(newIntervalData)
  }

  def updateWithEntries(entries: List[Entry]): IntervalsState = {
    IntervalsState(intervalData ++ entries)
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

  case class Entry(interval: Interval, lengthWeightTable: LengthWeightTable)
}
