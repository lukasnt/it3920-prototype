package com.lukasnt.spark.queries

import com.lukasnt.spark.queries.LengthWeightTable.Entry
import org.apache.spark.graphx.VertexId

class LengthWeightTable(val topK: Int = 10) extends Serializable {

  val tableData: List[Entry] = List()

  def updateWithEntry(entry: Entry): LengthWeightTable = {
    val newTableData = tableData :+ entry
    LengthWeightTable(newTableData)
  }

  def updateWithEntries(entries: List[Entry]): LengthWeightTable = {
    val newTableData = tableData ++ entries
    LengthWeightTable(newTableData)
  }

  def mergeWithTable(other: LengthWeightTable): LengthWeightTable = {
    val newTableData = tableData ++ other.tableData
    LengthWeightTable(newTableData)
  }

  override def toString: String = {
    s"[${tableData.mkString(", ")}]"
  }

}

object LengthWeightTable {

  def apply(data: List[Entry]): LengthWeightTable = LengthWeightTable(data, 10)

  def apply(data: List[Entry], topK: Int): LengthWeightTable = new LengthWeightTable(topK) {
    override val tableData: List[Entry] = data.sortBy(_.weight).reverse.take(topK)
  }

  case class Entry(length: Int, weight: Float, parentId: VertexId)
}
