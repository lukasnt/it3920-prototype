package com.lukasnt.spark.executors

import com.lukasnt.spark.io.Loggers
import com.lukasnt.spark.models.Types._
import com.lukasnt.spark.queries._
import com.lukasnt.spark.util.{IntervalStates, LengthWeightTable}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

class ParameterPregel(parameterQuery: ParameterQuery) extends PregelExecutor[PregelVertex, Properties, IntervalStates] {

  // Extract query parameters (because ParameterQuery is a non-serializable class)
  private val sourcePredicate: AttrVertex => Boolean             = parameterQuery.sourcePredicate
  private val intermediatePredicate: AttrEdge => Boolean         = parameterQuery.intermediatePredicate
  private val destinationPredicate: AttrVertex => Boolean        = parameterQuery.destinationPredicate
  private val validEdgeInterval: (Interval, Interval) => Boolean = parameterQuery.temporalPathType.validEdgeInterval
  private val nextInterval: (Interval, Interval) => Interval     = parameterQuery.temporalPathType.nextInterval
  private val initInterval: Interval => Interval                 = parameterQuery.temporalPathType.initInterval
  private val weightMap: AttrEdge => Float                       = parameterQuery.weightMap
  private val minLength: Int                                     = parameterQuery.minLength
  private val maxLength: Int                                     = parameterQuery.maxLength
  private val topK: Int                                          = parameterQuery.topK

  override def maxIterations(): Int = maxLength + 1

  override def activeDirection(): EdgeDirection = EdgeDirection.Out

  override def initMessages(): IntervalStates = {
    IntervalStates(List())
  }

  override def preprocessGraph(temporalGraph: TemporalGraph): Graph[PregelVertex, Properties] = {
    // Map to temporal pregel graph
    temporalGraph.mapVertices(
      (id, attr) =>
        PregelVertex(
          constState = ConstState
            .builder()
            .applySourceTest(vertexAttr => sourcePredicate(AttrVertex(id, vertexAttr)), attr)
            .applyIntermediateTest(_ => true, attr)
            .applyDestinationTest(vertexAttr => destinationPredicate(AttrVertex(id, vertexAttr)), attr)
            .build(),
          intervalStates = IntervalStates(List())
      )
    )
  }

  override def vertexProgram(vertexId: VertexId,
                             currentState: PregelVertex,
                             mergedMessage: IntervalStates): PregelVertex = {

    Loggers.default.debug(
      s"VERTEX_PROGRAM -- " +
        s"id: $vertexId, " +
        s"iterations: ${currentState.constState.iterations}, " +
        s"currentLength: ${currentState.constState.currentLength}, " +
        s"incomingLength: ${mergedMessage.currentLength}, " +
        s"intervalStates: ${currentState.intervalStates}, " +
        s"mergedMessage: $mergedMessage"
    )

    val newConstState = ConstState
      .builder()
      .fromState(currentState.constState)
      .incIterations()
      .setCurrentLength(mergedMessage.currentLength)
      .build()
    val newStates = currentState.intervalStates.mergeStates(mergedMessage.flushedTableStates, topK)

    PregelVertex(newConstState, newStates)
  }

  override def sendMessage(triplet: EdgeTriplet[PregelVertex, Properties]): Iterator[(VertexId, IntervalStates)] = {
    val iterations = triplet.srcAttr.constState.iterations
    // Make sure that in the first iteration only the source vertices send messages
    if (iterations <= 1 && !triplet.srcAttr.constState.source) {
      return Iterator.empty
    }

    val currentLength = triplet.srcAttr.constState.currentLength
    val currentStates = triplet.srcAttr.intervalStates
    val messageStates = IntervalStates(
      if (currentStates.intervalTables.nonEmpty)
        currentStates
          .intervalFilteredStates(validEdgeInterval, triplet.attr.interval)
          .lengthFilteredStates(currentLength)
          .intervalTables
          .map(intervalTable => messageIntervalTable(intervalTable, triplet, currentLength))
      else List(firstIntervalTable(triplet))
    )

    Loggers.default.debug(
      s"SEND_MESSAGE -- " +
        s"srcId: ${triplet.srcId}, " +
        s"dstId: ${triplet.dstId}, " +
        s"srcIterations: ${triplet.srcAttr.constState.iterations}, " +
        s"dstIterations: ${triplet.dstAttr.constState.iterations}, " +
        s"tripletInterval: ${triplet.attr.interval}, " +
        s"weight: ${triplet.attr.properties("weight")}, " +
        s"messageStates: $messageStates"
    )

    Iterator((triplet.dstId, messageStates))
  }

  private def messageIntervalTable(intervalTable: IntervalStates.IntervalTable,
                                   triplet: EdgeTriplet[PregelVertex, Properties],
                                   currentLength: Int) = {
    val newInterval = nextInterval(intervalTable.interval, triplet.attr.interval)
    val newTable = LengthWeightTable(
      history = List(),
      actives = intervalTable.table
        .filterByLength(currentLength, topK)
        .entries
        .map(
          entry =>
            LengthWeightTable.Entry(entry.length + 1,
                                    entry.weight + weightMap(AttrEdge(triplet.srcId, triplet.dstId, triplet.attr)),
                                    triplet.srcId)
        ),
      topK = topK
    )
    IntervalStates.IntervalTable(newInterval, newTable)
  }

  private def firstIntervalTable(triplet: EdgeTriplet[PregelVertex, Properties]): IntervalStates.IntervalTable = {
    IntervalStates.IntervalTable(
      interval = initInterval(triplet.attr.interval),
      table = LengthWeightTable(
        history = List(),
        actives = List(
          LengthWeightTable.Entry(1, weightMap(AttrEdge(triplet.srcId, triplet.dstId, triplet.attr)), triplet.srcId)
        ),
        topK = topK
      )
    )
  }

  override def mergeMessage(msgA: IntervalStates, msgB: IntervalStates): IntervalStates = {
    val merged = msgA.mergeStates(msgB, topK)
    Loggers.default.debug(s"MERGE_MESSAGE -- msgA: $msgA, msgB: $msgB, merged: $merged")
    merged
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
