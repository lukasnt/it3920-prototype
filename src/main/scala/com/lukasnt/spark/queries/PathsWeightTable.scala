package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.queries.PathsWeightTable.Entry

class PathsWeightTable extends Serializable {

  val tableData: List[Entry] = List()

  def updateWithEntry(entry: Entry, topK: Int): PathsWeightTable = {
    val newTableData = tableData :+ entry
    PathsWeightTable(newTableData, topK)
  }

  def updateWithEntries(entries: List[Entry], topK: Int): PathsWeightTable = {
    val newTableData = tableData ++ entries
    PathsWeightTable(newTableData, topK)
  }

  def mergeWithTable(other: PathsWeightTable, topK: Int): PathsWeightTable = {
    val newTableData = tableData ++ other.tableData
    PathsWeightTable(newTableData, topK)
  }

  def concatPathsWithTable(other: PathsWeightTable, topK: Int): PathsWeightTable = {
    val newTableData = for {
      entry1 <- tableData
      entry2 <- other.tableData
    } yield {
      val newPath   = entry1.path + entry2.path
      val newWeight = entry1.weight + entry2.weight
      Entry(newPath, newWeight)
    }
    PathsWeightTable(newTableData, topK)
  }
}

object PathsWeightTable {

  def apply(data: List[Entry], topK: Int): PathsWeightTable = new PathsWeightTable {
    override val tableData: List[Entry] = data.sortBy(_.weight).take(topK)
  }

  case class Entry(path: TemporalPath, weight: Float) {
    override def toString: String = {
      s"Entry($path|$weight)"
    }
  }

}
