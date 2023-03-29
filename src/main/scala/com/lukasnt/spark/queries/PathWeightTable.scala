package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.queries.PathWeightTable.Entry

class PathWeightTable extends Serializable {

  val entries: List[Entry] = List()

  def updateWithEntry(entry: Entry, topK: Int): PathWeightTable = {
    val newTableData = entries :+ entry
    PathWeightTable(newTableData, topK)
  }

  def updateWithEntries(entries: List[Entry], topK: Int): PathWeightTable = {
    val newTableData = entries ++ entries
    PathWeightTable(newTableData, topK)
  }

  def mergeWithTable(other: PathWeightTable, topK: Int): PathWeightTable = {
    val newTableData = entries ++ other.entries
    PathWeightTable(newTableData, topK)
  }

}

object PathWeightTable {

  def apply(pathsWeightTable: PathWeightTable): PathWeightTable = new PathWeightTable {
    override val entries: List[Entry] = pathsWeightTable.entries
  }

  def apply(tableEntries: List[Entry], topK: Int): PathWeightTable = new PathWeightTable {
    override val entries: List[Entry] = tableEntries.sortBy(_.remainingWeight).take(topK)
  }

  case class Entry(path: TemporalPath, remainingWeight: Float, remainingLength: Int) {
    override def toString: String = {
      s"Entry($path|$remainingWeight|$remainingLength)"
    }
    
  }

}
