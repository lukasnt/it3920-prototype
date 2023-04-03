package com.lukasnt.spark.queries

import com.lukasnt.spark.queries.LengthWeightTable.Entry
import org.apache.spark.graphx.VertexId

class LengthWeightTable() extends Serializable {

  val historyEntries: List[Entry] = List()
  val activeEntries: List[Entry]  = List()

  def updateWithEntry(entry: Entry, topK: Int): LengthWeightTable = {
    val newTableData = activeEntries :+ entry
    LengthWeightTable(this, newTableData, topK)
  }

  def updateWithEntries(entries: List[Entry], topK: Int): LengthWeightTable = {
    val newTableData = activeEntries ++ entries
    LengthWeightTable(this, newTableData, topK)
  }

  def mergeWithTable(other: LengthWeightTable, topK: Int): LengthWeightTable = {
    val newTableData = activeEntries ++ other.activeEntries
    LengthWeightTable(this, newTableData, topK)
  }

  def flushActiveEntries(): LengthWeightTable = {
    val newHistory = historyEntries ++ activeEntries
    LengthWeightTable(newHistory, List(), 10)
  }

  def filterByLengthRange(minLength: Int, maxLength: Int, topK: Int): LengthWeightTable = {
    val newActiveEntries  = activeEntries.filter(entry => entry.length >= minLength && entry.length <= maxLength)
    val newHistoryEntries = historyEntries.filter(entry => entry.length >= minLength && entry.length <= maxLength)
    LengthWeightTable(newHistoryEntries, newActiveEntries, topK)
  }

  def filterByLength(length: Int, topK: Int): LengthWeightTable = {
    val newActiveEntries  = activeEntries.filter(entry => entry.length == length)
    val newHistoryEntries = historyEntries.filter(entry => entry.length == length)
    LengthWeightTable(newHistoryEntries, newActiveEntries, topK)
  }

  def currentLength: Int =
    if (activeEntries.nonEmpty) activeEntries.map(_.length).max
    else if (historyEntries.nonEmpty) historyEntries.map(_.length).max
    else 0

  def minEntry: Option[Entry] = {
    if (activeEntries.nonEmpty) Some(activeEntries.minBy(_.weight))
    else if (historyEntries.nonEmpty) Some(historyEntries.minBy(_.weight))
    else None
  }

  def size: Int = entries.size

  def entries: List[Entry] = {
    historyEntries ++ activeEntries
  }

  override def toString: String = {
    s"(history=[${historyEntries.mkString(", ")}], actives=[${activeEntries.mkString(", ")}])"
  }

}

object LengthWeightTable {

  def apply(existingTable: LengthWeightTable, activeEntries: List[Entry], topK: Int): LengthWeightTable =
    LengthWeightTable(existingTable.historyEntries, activeEntries, topK)

  def apply(history: List[Entry], actives: List[Entry], topK: Int): LengthWeightTable = new LengthWeightTable() {
    override val historyEntries: List[Entry] = history
    override val activeEntries: List[Entry] = {
      if (topK == -1) actives.sortBy(_.weight)
      else actives.sortBy(_.weight).take(topK)
    }
  }

  case class Entry(length: Int, weight: Float, parentId: VertexId) {
    override def toString: String = {
      s"Entry($length|$weight|$parentId)"
    }
  }
}
