package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types._
import com.lukasnt.spark.queries._
import com.lukasnt.spark.utils.Loggers
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.util.Random

class ParameterPregel(parameterQuery: ParameterQuery)
    extends PregelExecutor[PregelVertex, Properties, IntervalMessage] {

  // Extract query parameters (because ParameterQuery is a non-serializable class)
  val sourcePredicate: AttrVertex => Boolean             = parameterQuery.sourcePredicate
  val intermediatePredicate: AttrEdge => Boolean         = parameterQuery.intermediatePredicate
  val destinationPredicate: AttrVertex => Boolean        = parameterQuery.destinationPredicate
  val intervalRelation: (Interval, Interval) => Interval = parameterQuery.intervalRelation
  val weightMap: AttrEdge => Float                       = parameterQuery.weightMap
  val minLength: Int                                     = parameterQuery.minLength
  val maxLength: Int                                     = parameterQuery.maxLength
  val topK: Int                                          = parameterQuery.topK

  override def maxIterations(): Int = 100

  override def activeDirection(): EdgeDirection = EdgeDirection.Out

  override def initMessages(): IntervalMessage = {
    IntervalMessage(interval = TemporalInterval(),
                    length = 0,
                    LengthWeightTable(List(
                                        LengthWeightTable.Entry(0, Random.nextFloat(), 0)
                                      ),
                                      topK = topK))
  }

  override def preprocessGraph(temporalGraph: TemporalGraph): Graph[PregelVertex, Properties] = {
    val initConstState = ConstState.builder().withSeqNum(0).build()

    // Map to temporal pregel graph
    temporalGraph.mapVertices(
      (_, _) =>
        PregelVertex(
          initConstState,
          IntervalsState(
            List(
              IntervalsState.Entry(TemporalInterval(),
                                   LengthWeightTable(
                                     List(
                                       LengthWeightTable.Entry(0, Random.nextFloat(), 0)
                                     ),
                                     topK = topK
                                   ))
            )
          )
      )
    )
  }

  override def vertexProgram(vertexId: VertexId,
                             currentState: PregelVertex,
                             mergedMessage: IntervalMessage): PregelVertex = {

    Loggers.default.debug(
      s"id: $vertexId, " +
        s"superstep: ${currentState.constState.superstep}, " +
        s"firstEntry: ${currentState.intervalsState.intervalData.head}, " +
        s"mergedMessage: $mergedMessage"
    )

    val newConstState = ConstState
      .builder()
      .fromState(currentState.constState)
      .incSuperstep()
      .applyPathCostUpdate(currentState.constState.pathCost - Random.nextFloat())
      .build()
    val newIntervalsState = currentState.intervalsState
      .updateWithEntry(
        IntervalsState.Entry(
          mergedMessage.interval,
          mergedMessage.lengthWeightTable.updateWithEntry(
            LengthWeightTable.Entry(mergedMessage.length, Random.nextFloat(), vertexId)
          )
        )
      )

    PregelVertex(newConstState, newIntervalsState)
  }

  override def sendMessage(triplet: EdgeTriplet[PregelVertex, Properties]): Iterator[(VertexId, IntervalMessage)] = {
    // Make sure that in the first iteration only the source vertices send messages
    if (triplet.srcAttr.constState.superstep == 0 && !triplet.srcAttr.constState.source) {
      return Iterator.empty
    }

    val interval = triplet.attr.interval
    val length   = triplet.srcAttr.constState.superstep
    val table    = LengthWeightTable(triplet.srcAttr.intervalsState.intervalData.head.lengthWeightTable.tableData)

    Loggers.default.debug(
      s"srcId: ${triplet.srcId}, " +
        s"dstId: ${triplet.dstId}, " +
        s"srcSuperstep: ${triplet.srcAttr.constState.superstep}, " +
        s"dstSuperstep: ${triplet.dstAttr.constState.superstep}")

    Iterator((triplet.dstId, IntervalMessage(interval, length, table)))
  }

  override def mergeMessage(msgA: IntervalMessage, msgB: IntervalMessage): IntervalMessage = {
    val interval    = msgA.interval.getUnion(msgB.interval)
    val length      = Math.max(msgA.length, msgB.length)
    val mergedTable = msgA.lengthWeightTable.mergeWithTable(msgB.lengthWeightTable)

    IntervalMessage(interval, length, mergedTable)
  }

}

object ParameterPregel {

  def apply(temporalGraph: TemporalGraph, parameterQuery: ParameterQuery): Graph[PregelVertex, Properties] = {
    new ParameterPregel(parameterQuery).run(temporalGraph)
  }

  def apply(temporalGraph: TemporalGraph): Graph[PregelVertex, Properties] = {
    new ParameterPregel(new ParameterQuery()).run(temporalGraph)
  }

}
