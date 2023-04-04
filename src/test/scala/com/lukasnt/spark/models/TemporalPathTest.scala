package com.lukasnt.spark.models

import org.apache.spark.graphx.Edge
import org.junit.Assert.assertTrue
import org.junit.Test

@Test
class TemporalPathTest {

  @Test
  def pathEquals(): Unit = {
    val path1 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path2 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    assertTrue(path1.equals(path2))
    assertTrue(path2.equals(path1))
  }

  @Test
  def startNode(): Unit = {
    val path1 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path2 = new TemporalPath(List(Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path3 = new TemporalPath(List(Edge(3, 4), Edge(4, 5)))
    val path4 = new TemporalPath(List(Edge(4, 5)))

    assertTrue(path1.startNode == 1)
    assertTrue(path2.startNode == 2)
    assertTrue(path3.startNode == 3)
    assertTrue(path4.startNode == 4)
  }

  @Test
  def endNode(): Unit = {
    val path1 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path2 = new TemporalPath(List(Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path3 = new TemporalPath(List(Edge(3, 4), Edge(4, 5)))
    val path4 = new TemporalPath(List(Edge(4, 5)))

    assertTrue(path1.endNode == 5)
    assertTrue(path2.endNode == 5)
    assertTrue(path3.endNode == 5)
    assertTrue(path4.endNode == 5)
  }

  @Test
  def pathAddition(): Unit = {
    val path1 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path2 = new TemporalPath(List(Edge(5, 6), Edge(6, 7), Edge(7, 8), Edge(8, 9)))
    val path3 = new TemporalPath(
      List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5), Edge(5, 6), Edge(6, 7), Edge(7, 8), Edge(8, 9)))
    assertTrue(path1 + path2 == path3)
  }

  @Test
  def edgeAddition(): Unit = {
    val path1 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path2 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5), Edge(5, 6)))
    assertTrue(path1 :+ Edge(5, 6) == path2)
  }

  @Test
  def startNodeAfterAddition(): Unit = {
    val path1 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path2 = new TemporalPath(List(Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path3 = new TemporalPath(List(Edge(3, 4), Edge(4, 5)))
    val path4 = new TemporalPath(List(Edge(4, 5)))

    assertTrue(path1.startNode == (path1 + path2).startNode)
    assertTrue(path2.startNode == (path2 + path3).startNode)
    assertTrue(path3.startNode == (path3 + path4).startNode)
  }

  @Test
  def endNodeAfterAddition(): Unit = {
    val path1 = new TemporalPath(List(Edge(1, 2), Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path2 = new TemporalPath(List(Edge(2, 3), Edge(3, 4), Edge(4, 5)))
    val path3 = new TemporalPath(List(Edge(3, 4), Edge(4, 5)))
    val path4 = new TemporalPath(List(Edge(4, 5)))

    assertTrue(path2.endNode == (path1 + path2).endNode)
    assertTrue(path3.endNode == (path2 + path3).endNode)
    assertTrue(path4.endNode == (path3 + path4).endNode)
  }

}
