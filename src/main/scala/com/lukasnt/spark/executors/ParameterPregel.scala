package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types._
import com.lukasnt.spark.queries._
import com.lukasnt.spark.utils.Loggers
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

class ParameterPregel(parameterQuery: ParameterQuery) extends PregelExecutor[PregelVertex, Properties, IntervalStates] {

  // Extract query parameters (because ParameterQuery is a non-serializable class)
  val sourcePredicate: AttrVertex => Boolean             = parameterQuery.sourcePredicate
  val intermediatePredicate: AttrEdge => Boolean         = parameterQuery.intermediatePredicate
  val destinationPredicate: AttrVertex => Boolean        = parameterQuery.destinationPredicate
  val validEdgeInterval: (Interval, Interval) => Boolean = parameterQuery.temporalPathType.validEdgeInterval
  val nextInterval: (Interval, Interval) => Interval     = parameterQuery.temporalPathType.nextInterval
  val initInterval: Interval => Interval                 = parameterQuery.temporalPathType.initInterval
  val weightMap: AttrEdge => Float                       = parameterQuery.weightMap
  val minLength: Int                                     = parameterQuery.minLength
  val maxLength: Int                                     = parameterQuery.maxLength
  val topK: Int                                          = parameterQuery.topK

  override def maxIterations(): Int = 100

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
      s"id: $vertexId, " +
        s"superstep: ${currentState.constState.superstep}, " +
        s"firstEntry: ${currentState.intervalStates.firstTable}, " +
        s"mergedMessage: $mergedMessage"
    )

    val newConstState = ConstState
      .builder()
      .fromState(currentState.constState)
      .incSuperstep()
      .build()
    val newStates = currentState.intervalStates.updateWithTables(mergedMessage.flushedTableStates.intervalTables)

    PregelVertex(newConstState, newStates)
  }

  override def sendMessage(triplet: EdgeTriplet[PregelVertex, Properties]): Iterator[(VertexId, IntervalStates)] = {
    val superstep = triplet.srcAttr.constState.superstep
    // Make sure that in the first iteration only the source vertices send messages
    if (superstep <= 1 && !triplet.srcAttr.constState.source) {
      return Iterator.empty
    }

    val currentStates = triplet.srcAttr.intervalStates
    val messageStates = IntervalStates(
      if (currentStates.intervalTables.nonEmpty)
        currentStates
          .intervalFilteredStates(validEdgeInterval, triplet.attr.interval)
          .intervalTables
          .map(intervalTable => messageIntervalTable(intervalTable, triplet, superstep))
      else List(firstIntervalTable(triplet))
    )

    Loggers.default.debug(
      s"srcId: ${triplet.srcId}, " +
        s"dstId: ${triplet.dstId}, " +
        s"srcSuperstep: ${triplet.srcAttr.constState.superstep}, " +
        s"dstSuperstep: ${triplet.dstAttr.constState.superstep}, " +
        s"tripletInterval: ${triplet.attr.interval}, " +
        s"weight: ${triplet.attr.properties("weight")}, " +
        s"messageStates: $messageStates"
    )

    Iterator((triplet.dstId, messageStates))
  }

  private def messageIntervalTable(intervalTable: IntervalStates.IntervalTable,
                                   triplet: EdgeTriplet[PregelVertex, Properties],
                                   superstep: Int) = {
    val newInterval = nextInterval(intervalTable.interval, triplet.attr.interval)
    val newTable = LengthWeightTable(
      history = List(),
      actives = intervalTable.table
        .filterByLength(superstep - 1, topK)
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
    msgA.mergeStates(msgB, topK)
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
