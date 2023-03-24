package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types.{PregelVertex, Properties, TemporalGraph}
import com.lukasnt.spark.queries.{ConstState, IntervalMessage, IntervalsState, LengthWeightTable}
import com.lukasnt.spark.utils.Loggers
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.util.Random

class TemplatePregelRunner extends PregelRunner[PregelVertex, Properties, IntervalMessage] {

  override def maxIterations(): Int = 100

  override def activeDirection(): EdgeDirection = EdgeDirection.Either

  override def initMessages(): IntervalMessage =
    IntervalMessage(TemporalInterval(),
                    0,
                    LengthWeightTable(
                      List(
                        LengthWeightTable.Entry(0, Random.nextFloat(), 0)
                      )))

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
                                     )
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
      s"id: $vertexId, superstep: " +
        s"${currentState.constState.superstep}, firstEntry: " +
        s"${currentState.intervalsState.intervalData.head}"
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
    val length   = triplet.srcAttr.constState.superstep
    val interval = triplet.attr.interval

    Loggers.default.debug(
      s"srcId: ${triplet.srcId}, " +
        s"dstId: ${triplet.dstId}, " +
        s"srcSuperstep: ${triplet.srcAttr.constState.superstep}, " +
        s"dstSuperstep: ${triplet.dstAttr.constState.superstep}")

    Iterator((triplet.dstId, IntervalMessage(interval, length, LengthWeightTable(List()))))
  }

  override def mergeMessage(msg1: IntervalMessage, msg2: IntervalMessage): IntervalMessage = {
    msg1
  }
}

object TemplatePregelRunner {
  def apply(temporalGraph: TemporalGraph): Graph[PregelVertex, Properties] = {
    new TemplatePregelRunner().run(temporalGraph)
  }
}
