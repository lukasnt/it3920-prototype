package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.{AttrVertex, Interval, Properties, TemporalGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime

class TemporalPath(val edgeSequence: List[Edge[Properties]]) extends Serializable {

  def asTemporalGraph(sc: SparkContext): TemporalGraph = {
    Graph.apply(
      sc.parallelize(edgeSequence.map(edge =>
        (edge.srcId, new Properties(edge.attr.interval, edge.attr.typeLabel, edge.attr.properties)))),
      sc.parallelize(edgeSequence)
    )
  }

  def asTemporalGraph(originalGraph: TemporalGraph): TemporalGraph = {
    val sequenceVertices = edgeSequence.flatMap(edge => List(edge.srcId, edge.dstId)).distinct
    Graph.apply(
      originalGraph.vertices.filter(v => sequenceVertices.contains(AttrVertex(v).id)),
      originalGraph.edges.filter(e => edgeSequence.exists(se => e.srcId == se.srcId && e.dstId == se.dstId))
    )
  }

  def outerJoinWithEdges(edges: List[Edge[Properties]]): List[TemporalPath] = {
    edges.map(edge => this :+ edge)
  }

  def innerJoinWithEdges(edges: RDD[Edge[Properties]]): RDD[TemporalPath] = {
    edges.filter(edge => edge.srcId == endNode).map(edge => this :+ edge)
  }

  def :+(edge: Edge[Properties]): TemporalPath = {
    new TemporalPath(edgeSequence :+ edge)
  }

  def outerJoinWithPaths(paths: List[TemporalPath]): List[TemporalPath] = {
    paths.map(path => this + path)
  }

  def +(path: TemporalPath): TemporalPath = {
    new TemporalPath(edgeSequence ++ path.edgeSequence)
  }

  def innerJoinWithPaths(paths: List[TemporalPath]): List[TemporalPath] = {
    paths.filter(path => path.startNode == endNode).map(path => this + path)
  }

  def endNode: Long = {
    edgeSequence.last.dstId
  }

  def startNode: Long = {
    edgeSequence.head.srcId
  }

  def length: Int = edgeSequence.length

  def interval: Interval = {
    new TemporalInterval(startTimestamp, endTimestamp)
  }

  def startTimestamp: ZonedDateTime = {
    edgeSequence.head.attr.interval.startTime
  }

  def endTimestamp: ZonedDateTime = {
    edgeSequence.last.attr.interval.endTime
  }

  override def toString: String = {
    (edgeSequence.map(edge => edge.srcId) :+ edgeSequence.last.dstId).mkString("->")
  }

}

object TemporalPath {

  def apply(edgeSequence: List[Edge[Properties]]): TemporalPath = {
    new TemporalPath(edgeSequence)
  }

  def apply(edge: Edge[Properties]): TemporalPath = {
    new TemporalPath(List(edge))
  }

  def apply(): TemporalPath = {
    new TemporalPath(List())
  }

}
