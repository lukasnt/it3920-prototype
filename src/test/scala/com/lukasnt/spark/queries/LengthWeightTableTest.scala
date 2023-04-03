package com.lukasnt.spark.queries

import org.junit.Assert.assertTrue
import org.junit.Test

@Test
class LengthWeightTableTest {

  @Test
  def mergeTables(): Unit = {
    val topK = 3
    val table1 = LengthWeightTable(
      history = List(),
      actives = List(
        LengthWeightTable.Entry(1, 0.23423f, 25123L),
        LengthWeightTable.Entry(1, 1.23423f, 25123L)
      ),
      topK
    )
    val table2 = LengthWeightTable(
      history = List(),
      actives = List(
        LengthWeightTable.Entry(1, 0.23423f, 25123L),
        LengthWeightTable.Entry(1, 1.23423f, 25123L)
      ),
      topK
    )
    val result = LengthWeightTable(
      history = List(),
      actives = List(
        LengthWeightTable.Entry(1, 0.23423f, 25123L),
        LengthWeightTable.Entry(1, 0.23423f, 25123L),
        LengthWeightTable.Entry(1, 1.23423f, 25123L)
      ),
      topK
    )

    val merged = table1.mergeWithTable(table2, topK)
    assertTrue(merged == result)
    assertTrue(merged.activeEntries.length == 3)
  }

  @Test
  def reduceListOfTablesWithMerge(): Unit = {
    val topK = 3
    val tableList = List(
      LengthWeightTable(
        history = List(),
        actives = List(
          LengthWeightTable.Entry(1, 0.23423f, 25123L),
          LengthWeightTable.Entry(1, 1.23423f, 25123L)
        ),
        topK
      ),
      LengthWeightTable(
        history = List(),
        actives = List(
          LengthWeightTable.Entry(1, 0.23423f, 25123L),
          LengthWeightTable.Entry(1, 1.23423f, 25123L)
        ),
        topK
      ),
      LengthWeightTable(
        history = List(),
        actives = List(
          LengthWeightTable.Entry(1, 0.23423f, 25123L),
          LengthWeightTable.Entry(1, 0.23423f, 25123L),
          LengthWeightTable.Entry(1, 1.23423f, 25123L)
        ),
        topK
      )
    )

    val result = LengthWeightTable(
      history = List(),
      actives = List(
        LengthWeightTable.Entry(1, 0.23423f, 25123L),
        LengthWeightTable.Entry(1, 0.23423f, 25123L),
        LengthWeightTable.Entry(1, 0.23423f, 25123L)
      ),
      topK
    )

    val reducedTable = tableList.reduce((a, b) => a.mergeWithTable(b, topK))
    assertTrue(reducedTable == result)
    assertTrue(reducedTable.activeEntries.length == 3
    )
  }
}
