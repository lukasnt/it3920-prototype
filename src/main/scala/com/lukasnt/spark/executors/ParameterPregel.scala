package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types._
import com.lukasnt.spark.queries._
import com.lukasnt.spark.util.{IntervalStates, LengthWeightTable, QueryState}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

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

  //private var historyBuffers: ListBuffer[(VertexId, List[IntervalStates.IntervalTable])] = ListBuffer()

  override def maxIterations(): Int = maxLength

  override def activeDirection(): EdgeDirection = EdgeDirection.Out

  override def initMessages(): IntervalStates = {
    IntervalStates(HashMap())
  }

  override def preprocessGraph(temporalGraph: TemporalGraph): Graph[PregelVertex, Properties] = {
    // Map to temporal pregel graph
    temporalGraph.mapVertices(
      (id, attr) =>
        PregelVertex(
          queryState = QueryState
            .builder()
            .applySourceTest(vertexAttr => sourcePredicate(AttrVertex(id, vertexAttr)), attr)
            .applyIntermediateTest(_ => true, attr)
            .applyDestinationTest(vertexAttr => destinationPredicate(AttrVertex(id, vertexAttr)), attr)
            .build(),
          intervalStates = IntervalStates(HashMap())
      )
    )
  }

  override def vertexProgram(vertexId: VertexId,
                             currentState: PregelVertex,
                             mergedMessage: IntervalStates): PregelVertex = {

    /*Loggers.default.debug(
      s"VERTEX_PROGRAM -- " +
        s"id: $vertexId, " +
        s"iterations: ${currentState.queryState.iterations}, " +
        s"currentLength: ${currentState.queryState.currentLength}, " +
        s"incomingLength: ${mergedMessage.currentLength}, " +
        s"intervalStates: ${currentState.intervalStates}, " +
        s"mergedMessage: $mergedMessage"
    )*/

    //historyBuffers += ((vertexId, currentState.intervalStates.intervalTables))

    PregelVertex(
      queryState = QueryState
        .builder()
        .fromState(currentState.queryState)
        .incIterations()
        .build(),
      intervalStates = currentState.intervalStates
        .flushedTableStates(topK)
        .mergedStates(mergedMessage, topK)
    )
  }

  override def sendMessage(triplet: EdgeTriplet[PregelVertex, Properties]): Iterator[(VertexId, IntervalStates)] = {
    // Make sure that in the first iteration only the source vertices send messages
    val iterations = triplet.srcAttr.queryState.iterations
    if (iterations <= 1 && !triplet.srcAttr.queryState.source) {
      return Iterator.empty
    }

    val currentStates = triplet.srcAttr.intervalStates
    val messageStates = IntervalStates(
      if (currentStates.intervalTables.nonEmpty)
        currentStates
          .intervalFilteredStates(validEdgeInterval, triplet.attr.interval)
          .intervalTables
          .map(entry => messageIntervalTable(entry, triplet))
      else HashMap(firstIntervalTable(triplet))
    )

    /*Loggers.default.debug(
      s"SEND_MESSAGE -- " +
        s"srcId: ${triplet.srcId}, " +
        s"dstId: ${triplet.dstId}, " +
        s"srcIterations: ${triplet.srcAttr.queryState.iterations}, " +
        s"dstIterations: ${triplet.dstAttr.queryState.iterations}, " +
        s"tripletInterval: ${triplet.attr.interval}, " +
        s"weight: ${triplet.attr.properties("weight")}, " +
        s"messageStates: $messageStates"
    )*/

    Iterator((triplet.dstId, messageStates))
  }

  private def messageIntervalTable(intervalTableEntry: (Interval, LengthWeightTable),
                                   triplet: EdgeTriplet[PregelVertex, Properties]): (Interval, LengthWeightTable) = {
    val (interval: Interval, table: LengthWeightTable) = intervalTableEntry
    val newInterval                                    = nextInterval(interval, triplet.attr.interval)
    val newTable = LengthWeightTable(
      history = ArrayBuffer(),
      actives = table.activeEntries
        .map(
          entry =>
            LengthWeightTable.Entry(entry.length + 1,
                                    entry.weight + weightMap(AttrEdge(triplet.srcId, triplet.dstId, triplet.attr)),
                                    triplet.srcId)
        ),
      topK = -1
    )
    (newInterval, newTable)
  }

  private def firstIntervalTable(triplet: EdgeTriplet[PregelVertex, Properties]): (Interval, LengthWeightTable) = {
    (initInterval(triplet.attr.interval),
     LengthWeightTable(
       history = ArrayBuffer(),
       actives = ArrayBuffer(
         LengthWeightTable.Entry(1, weightMap(AttrEdge(triplet.srcId, triplet.dstId, triplet.attr)), triplet.srcId)
       ),
       topK = -1
     ))
  }

  override def mergeMessage(msgA: IntervalStates, msgB: IntervalStates): IntervalStates = {
    val merged = msgA.mergedStates(msgB, topK)
    /*Loggers.default.debug(s"MERGE_MESSAGE -- msgA: $msgA, msgB: $msgB, merged: $merged")*/
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
