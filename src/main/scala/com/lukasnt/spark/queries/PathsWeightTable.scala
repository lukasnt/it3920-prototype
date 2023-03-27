package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.queries.PathsWeightTable.Entry

class PathsWeightTable extends Serializable {

  val entries: List[Entry] = List()

  def updateWithEntry(entry: Entry, topK: Int): PathsWeightTable = {
    val newTableData = entries :+ entry
    PathsWeightTable(newTableData, topK)
  }

  def updateWithEntries(entries: List[Entry], topK: Int): PathsWeightTable = {
    val newTableData = entries ++ entries
    PathsWeightTable(newTableData, topK)
  }

  def mergeWithTable(other: PathsWeightTable, topK: Int): PathsWeightTable = {
    val newTableData = entries ++ other.entries
    PathsWeightTable(newTableData, topK)
  }

  def concatPathsWithTable(other: PathsWeightTable, topK: Int): PathsWeightTable = {
    val newTableData = for {
      entry1 <- entries
      entry2 <- other.entries
    } yield {
      val newPath   = entry1.path + entry2.path
      val newWeight = entry1.remainingWeight + entry2.remainingWeight
      Entry(newPath, newWeight, 0)
    }
    PathsWeightTable(newTableData, topK)
  }
}

object PathsWeightTable {

  def apply(pathsWeightTable: PathsWeightTable): PathsWeightTable = new PathsWeightTable {
    override val entries: List[Entry] = pathsWeightTable.entries
  }

  def apply(tableEntries: List[Entry], topK: Int): PathsWeightTable = new PathsWeightTable {
    override val entries: List[Entry] = tableEntries.sortBy(_.remainingWeight).take(topK)
  }

  case class Entry(path: TemporalPath, remainingWeight: Float, remainingLength: Int) {
    override def toString: String = {
      s"Entry($path|$remainingWeight|$remainingLength)"
    }
    
  }

}
