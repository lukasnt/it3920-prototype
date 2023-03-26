package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{Interval, Properties, TemporalGraph}
import com.lukasnt.spark.queries.{ConstState, ConstStateMessages, SequencedQueries}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

class SequencePregel(sequencedQueries: SequencedQueries)
    extends PregelExecutor[(Properties, List[ConstState]), Properties, ConstStateMessages] {

  // Extract the functions and values from the queries
  val aggTests: List[(Properties, Properties, Properties) => Boolean] = sequencedQueries.sequence.map(q => q._2.aggTest)
  val aggCosts: List[(Float, Properties) => Float]                    = sequencedQueries.sequence.map(q => q._2.aggCost)
  val aggIntervalTests: List[(Interval, Interval) => Boolean]         = sequencedQueries.sequence.map(q => q._2.aggIntervalTest)
  val nodeTests: List[Properties => Boolean]                          = sequencedQueries.sequence.map(q => q._1.nodeTest)
  val nodeCosts: List[Properties => Float]                            = sequencedQueries.sequence.map(q => q._1.nodeCost)
  val emptyStates: List[ConstState]                                   = sequencedQueries.createInitStates()
  val maxPathLength: Int                                              = sequencedQueries.sequence.length * 3

  override def maxIterations(): Int = maxPathLength

  override def activeDirection(): EdgeDirection = EdgeDirection.Out

  override def initMessages(): ConstStateMessages = {
    new ConstStateMessages(List(ConstState.builder().withSeqNum(-1).withInitPathCost(Float.MaxValue).build()))
  }

  override def preprocessGraph(temporalGraph: TemporalGraph): Graph[(Properties, List[ConstState]), Properties] = {
    // Map to temporal pregel graph
    temporalGraph.mapVertices((id, attr) => {
      val initStates =
        emptyStates.map(
          state =>
            ConstState
              .builder()
              .fromState(state)
              .applySourceTest(attr, nodeTests(state.seqNum))
              .applyNodeCost(attr, nodeCosts(state.seqNum))
              .applyPathCostUpdate(if (state.seqNum == 0) nodeCosts(state.seqNum)(attr) else Float.MaxValue)
              .build())
      (attr, initStates)
    })
  }

  override def vertexProgram(vertexId: VertexId,
                             currentState: (Properties, List[ConstState]),
                             message: ConstStateMessages): (Properties, List[ConstState]) = {
    val (node, stateSequence) = currentState
    val newStateSequence =
      stateSequence.map(
        state =>
          ConstState
            .builder()
            .fromState(state)
            .incSuperstep()
            .applyPathCostUpdate(message.getMinCostBy(state.seqNum - 1))
            .build())
    (node, newStateSequence)
  }

  override def sendMessage(
      triplet: EdgeTriplet[(Properties, List[ConstState]), Properties]): Iterator[(VertexId, ConstStateMessages)] = {
    val srcStateSequence = triplet.srcAttr._2

    /*
    Loggers.default.debug(
      s"srcSuperstep: ${triplet.srcAttr._2.apply(2).superstep}, " +
        s"dstSuperStep: ${triplet.dstAttr._2.apply(2).superstep}")
     */

    val messages = new ConstStateMessages(
      srcStateSequence
        .map(queryState => {
          val seqNum = queryState.seqNum
          ConstState
            .builder()
            .fromState(queryState)
            .applyWeightedPregelTriplet(triplet, aggTests(seqNum), aggIntervalTests(seqNum), aggCosts(seqNum))
            .build()
        })
      //.filter(newState => newState.testSuccess)
    )

    List((triplet.srcId, new ConstStateMessages(List.empty)), (triplet.dstId, messages)).toIterator
  }

  override def mergeMessage(msgA: ConstStateMessages, msgB: ConstStateMessages): ConstStateMessages = {
    msgA.merge(msgB)
  }
}

object SequencePregel {

  def apply(temporalGraph: TemporalGraph,
            sequencedQueries: SequencedQueries): Graph[(Properties, List[ConstState]), Properties] = {
    new SequencePregel(sequencedQueries).run(temporalGraph)
  }

}
