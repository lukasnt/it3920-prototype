package com.lukasnt.spark.util

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.Interval
import com.lukasnt.spark.util.PathWeightTable.Entry
import org.apache.spark.graphx.VertexId

class PathWeightTable extends Serializable {

  val entries: List[Entry] = List()

  def updateWithEntry(entry: Entry, topK: Int): PathWeightTable = {
    val newTableData = entries :+ entry
    PathWeightTable(newTableData, topK)
  }

  def mergeWithTable(other: PathWeightTable, topK: Int): PathWeightTable = {
    val newTableData = entries ++ other.entries
    PathWeightTable(newTableData, topK)
  }

  def parentVertexExists(vertexId: VertexId): Boolean = {
    entries.exists(_.parentVertex == vertexId)
  }

  def destinationVertexExists(vertexId: VertexId): Boolean = {
    entries.exists(_.destinationVertex == vertexId)
  }

  override def toString: String = {
    s"PathWeightTable([\n${entries.mkString("\n")}\n])"
  }

}

object PathWeightTable {

  def apply(pathsWeightTable: PathWeightTable): PathWeightTable = new PathWeightTable {
    override val entries: List[Entry] = pathsWeightTable.entries
  }

  def apply(tableEntries: List[Entry], topK: Int): PathWeightTable = new PathWeightTable {
    override val entries: List[Entry] = {
      if (topK == -1) tableEntries.sortBy(_.weight)
      else tableEntries.sortBy(_.weight).take(topK)
    }
  }

  case class Entry(interval: Interval, remainingLength: Int, weight: Float, path: TemporalPath) {
    override def toString: String = {
      s"Entry($interval|$remainingLength|$weight|$path)"
    }

    def parentVertex: VertexId = path.startNode

    def destinationVertex: VertexId = path.endNode

  }

}
