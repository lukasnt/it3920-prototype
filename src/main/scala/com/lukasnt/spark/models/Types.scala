package com.lukasnt.spark.models

import com.lukasnt.spark.queries.{ConstState, IntervalStates}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}

import java.time.temporal.Temporal
import java.time.{ZoneId, ZonedDateTime}

/**
  * Type aliases and wrappers for some of the models and spark classes.
  * ZonedDateTime is used as the default temporal type for instance.
  */
object Types {

  type Interval = TemporalInterval[ZonedDateTime]

  type Properties = TemporalProperties[ZonedDateTime]

  type GenericTemporalGraph[T <: Temporal] = Graph[TemporalProperties[T], TemporalProperties[T]]

  type TemporalGraph = Graph[Properties, Properties]

  case class AttrVertex(id: VertexId, attr: Properties)

  case class AttrEdge(srcId: VertexId, dstId: VertexId, attr: Properties)

  case class PregelVertex(constState: ConstState, intervalStates: IntervalStates)

  object AttrVertex {

    def apply(vertex: (VertexId, Properties)): AttrVertex = {
      new AttrVertex(vertex._1, vertex._2)
    }

  }

  object AttrEdge {

    def apply(edge: Edge[Properties]): AttrEdge = {
      new AttrEdge(edge.srcId, edge.dstId, edge.attr)
    }

    def apply(triplet: EdgeTriplet[Properties, Properties]): AttrEdge = {
      new AttrEdge(triplet.srcId, triplet.dstId, triplet.attr)
    }

  }

  object Interval {

    def apply(): Interval = {
      Interval(
        ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(0), ZoneId.systemDefault()),
        ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(0), ZoneId.systemDefault())
      )
    }

    def apply(interval: (ZonedDateTime, ZonedDateTime)): Interval = {
      new Interval(interval._1, interval._2)
    }

  }

}
