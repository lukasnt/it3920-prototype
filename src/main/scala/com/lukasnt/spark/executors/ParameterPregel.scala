package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types._
import com.lukasnt.spark.queries._
import com.lukasnt.spark.utils.Loggers
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

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
                    LengthWeightTable(history = List(),
                                      actives = List(
                                        LengthWeightTable.Entry(0, 0, 0)
                                      ),
                                      topK = topK))
  }

  override def preprocessGraph(temporalGraph: TemporalGraph): Graph[PregelVertex, Properties] = {
    // Map to temporal pregel graph
    temporalGraph.mapVertices(
      (id, attr) =>
        PregelVertex(
          constState = ConstState
            .builder()
            .applySourceTest(attr, vertexAttr => sourcePredicate(AttrVertex(id, vertexAttr)))
            .applyIntermediateTest(attr, _ => true)
            .applyDestinationTest(attr, vertexAttr => destinationPredicate(AttrVertex(id, vertexAttr)))
            .build(),
          intervalsState = IntervalsState(
            List(
              IntervalsState.Entry(
                interval = TemporalInterval(),
                lengthWeightTable = LengthWeightTable(
                  history = List(),
                  actives = List(),
                  topK = topK
                )
              )
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
        s"firstEntry: ${currentState.intervalsState.firstTable}, " +
        s"mergedMessage: $mergedMessage"
    )

    val currentTable = currentState.intervalsState.intervalData.head.lengthWeightTable

    val newConstState = ConstState
      .builder()
      .fromState(currentState.constState)
      .incSuperstep()
      .build()
    val newIntervalsState = currentState.intervalsState
      .updateWithEntry(
        IntervalsState.Entry(
          mergedMessage.interval,
          currentTable
            .mergeWithTable(mergedMessage.lengthWeightTable, topK)
            .flushActiveEntries()
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
    val table    = triplet.srcAttr.intervalsState.firstTable
    val length   = table.currentLength

    val messageInterval = interval
    val messageLength   = length + 1
    val messageTable = LengthWeightTable(
      history = List(),
      actives = table
        .filterByLength(length, topK)
        .entries
        .map(
          entry =>
            LengthWeightTable.Entry(messageLength,
                                    entry.weight + weightMap(AttrEdge(triplet.srcId, triplet.dstId, triplet.attr)),
                                    triplet.srcId)
        ),
      topK = topK
    )

    Loggers.default.debug(
      s"srcId: ${triplet.srcId}, " +
        s"dstId: ${triplet.dstId}, " +
        s"srcSuperstep: ${triplet.srcAttr.constState.superstep}, " +
        s"dstSuperstep: ${triplet.dstAttr.constState.superstep}, " +
        s"interval: $messageInterval, " +
        s"length: $messageLength, " +
        s"table: $table, " +
        s"messageTable: $messageTable, " +
        s"weight: ${triplet.attr.properties("weight")}"
    )

    Iterator((triplet.dstId, IntervalMessage(messageInterval, messageLength, messageTable)))
  }

  override def mergeMessage(msgA: IntervalMessage, msgB: IntervalMessage): IntervalMessage = {
    val interval    = msgA.interval.getUnion(msgB.interval)
    val length      = Math.max(msgA.length, msgB.length)
    val mergedTable = msgA.lengthWeightTable.mergeWithTable(msgB.lengthWeightTable, topK)

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
