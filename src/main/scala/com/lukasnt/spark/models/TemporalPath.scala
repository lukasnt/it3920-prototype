package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.{Interval, TemporalGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime

class TemporalPath(val edgeSequence: List[Edge[TemporalProperties[ZonedDateTime]]]) extends Serializable {

  def asTemporalGraph(sc: SparkContext): TemporalGraph = {
    Graph.apply(
      sc.parallelize(
        edgeSequence.map(edge =>
          (edge.srcId,
           new TemporalProperties[ZonedDateTime](edge.attr.interval, edge.attr.typeLabel, edge.attr.properties)))),
      sc.parallelize(edgeSequence)
    )
  }

  def asTemporalGraph(originalGraph: TemporalGraph): TemporalGraph = {
    val sequenceVertices = edgeSequence.flatMap(edge => List(edge.srcId, edge.dstId)).distinct
    Graph.apply(
      originalGraph.vertices.filter(v => sequenceVertices.contains(v._1)),
      originalGraph.edges.filter(e => edgeSequence.contains(e))
    )
  }

  def outerJoinWithEdges(edges: List[Edge[TemporalProperties[ZonedDateTime]]]): List[TemporalPath] = {
    edges.map(edge => concatWithEdge(edge))
  }

  def innerJoinWithEdges(edges: RDD[Edge[TemporalProperties[ZonedDateTime]]]): RDD[TemporalPath] = {
    edges.filter(edge => edge.srcId == getEndNode).map(edge => concatWithEdge(edge))
  }

  def concatWithEdge(edge: Edge[TemporalProperties[ZonedDateTime]]): TemporalPath = {
    new TemporalPath(edgeSequence :+ edge)
  }

  def getEndNode: Long = {
    edgeSequence.last.dstId
  }

  def outerJoinWithPaths(paths: List[TemporalPath]): List[TemporalPath] = {
    paths.map(path => concatWithPath(path))
  }

  def concatWithPath(path: TemporalPath): TemporalPath = {
    new TemporalPath(edgeSequence ++ path.edgeSequence)
  }

  def innerJoinWithPaths(paths: List[TemporalPath]): List[TemporalPath] = {
    paths.filter(path => path.getStartNode == getEndNode).map(path => concatWithPath(path))
  }

  def getStartNode: Long = {
    edgeSequence.head.srcId
  }

  def getInterval: Interval = {
    new TemporalInterval(getStartTimestamp, getEndTimestamp)
  }

  def getStartTimestamp: ZonedDateTime = {
    edgeSequence.head.attr.interval.startTime
  }

  def getEndTimestamp: ZonedDateTime = {
    edgeSequence.last.attr.interval.endTime
  }

  override def toString: String = {
    edgeSequence.flatMap(edge => List(edge.srcId, edge.dstId)).mkString(" -> ")
  }

}
