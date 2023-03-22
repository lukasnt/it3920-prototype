package com.lukasnt.spark.models

import com.lukasnt.spark.queries.{ConstState, IntervalsState}
import org.apache.spark.graphx.{Graph, VertexId}

import java.time.ZonedDateTime
import java.time.temporal.Temporal

/**
  * Type aliases for the models classes.
  * ZonedDateTime is used as the default temporal type.
  */
object Types {

  type Interval = TemporalInterval[ZonedDateTime]

  type Properties = TemporalProperties[ZonedDateTime]

  type GenericTemporalGraph[T <: Temporal] = Graph[TemporalProperties[T], TemporalProperties[T]]

  type TemporalGraph = GenericTemporalGraph[ZonedDateTime]

  type PregelGraph = Graph[PregelVertex, Properties]

  type SequencedPregelGraph = Graph[(Properties, List[ConstState]), Properties]

  case class AttrVertex(id: VertexId, attr: Properties)

  case class AttrEdge(srcId: VertexId, dstId: VertexId, attr: Properties)

  case class PregelVertex(constState: ConstState, intervalsState: IntervalsState)

}
