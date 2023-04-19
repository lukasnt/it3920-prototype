package com.lukasnt.spark.util

import com.lukasnt.spark.util.LengthWeightTable.Entry
import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ArrayBuffer

class LengthWeightTable() extends Serializable {

  val historyEntries: ArrayBuffer[Entry] = ArrayBuffer()
  val activeEntries: ArrayBuffer[Entry]  = ArrayBuffer()

  def updateWithEntry(entry: Entry, topK: Int): LengthWeightTable = {
    activeEntries += entry
    LengthWeightTable(historyEntries, activeEntries, topK)
  }

  def mergeWithTable(other: LengthWeightTable, topK: Int): LengthWeightTable = {
    LengthWeightTable(historyEntries, activeEntries ++ other.activeEntries, topK)
  }

  def flushActiveEntries(topK: Int): LengthWeightTable = {
    LengthWeightTable(historyEntries ++ activeEntries, ArrayBuffer(), topK)
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
    historyEntries.toList ++ activeEntries
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

  def apply(history: ArrayBuffer[Entry], actives: ArrayBuffer[Entry], topK: Int): LengthWeightTable =
    new LengthWeightTable() {
      override val historyEntries: ArrayBuffer[Entry] = {
        history
      }
      override val activeEntries: ArrayBuffer[Entry] = {
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
