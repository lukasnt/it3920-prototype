package com.lukasnt.spark.util

import com.lukasnt.spark.util.LengthWeightTable.Entry
import org.apache.spark.graphx.VertexId

class LengthWeightTable() extends Serializable {

  val historyEntries: List[Entry] = List()
  val activeEntries: List[Entry]  = List()

  def updateWithEntry(entry: Entry, topK: Int): LengthWeightTable = {
    val newTableData = activeEntries :+ entry
    LengthWeightTable(this, newTableData, topK)
  }

  def mergeWithTable(other: LengthWeightTable, topK: Int): LengthWeightTable = {
    val newActiveEntries = activeEntries ++ other.activeEntries
    LengthWeightTable(this, newActiveEntries, topK)
  }

  def flushActiveEntries(topK: Int): LengthWeightTable = {
    val newHistory = historyEntries ++ activeEntries
    LengthWeightTable(newHistory, List(), topK)
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

  override def equals(other: Any): Boolean = other match {
    case that: LengthWeightTable =>
      historyEntries == that.historyEntries &&
        activeEntries == that.activeEntries
    case _ => false
  }

  override def toString: String = {
    s"(history=[${historyEntries.mkString(", ")}], actives=[${activeEntries.mkString(", ")}])"
  }

}

object LengthWeightTable {

  def apply(existingTable: LengthWeightTable, newActiveEntries: List[Entry], topK: Int): LengthWeightTable =
    new LengthWeightTable() {
      override val historyEntries: List[Entry] = existingTable.historyEntries
      override val activeEntries: List[Entry] = {
        if (topK == -1) newActiveEntries.sortBy(_.weight)
        else newActiveEntries.sortBy(_.weight).take(topK)
      }
    }

  def apply(history: List[Entry], actives: List[Entry], topK: Int): LengthWeightTable = new LengthWeightTable() {
    override val historyEntries: List[Entry] = {
      if (topK == -1) history.sortBy(_.weight)
      else history.sortBy(_.weight).take(topK)
    }
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
