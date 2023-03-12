package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{TemporalGraph, TemporalPregelGraph}
import com.lukasnt.spark.models.{QueryState, QueryStateMessages, SequencedQueries, WeightedQueries}
import org.apache.spark.graphx.EdgeDirection

object WeightedPregelRunner {

  def run(weightedQueries: WeightedQueries, temporalGraph: TemporalGraph): TemporalPregelGraph = {

    // Initialize the temporal pregel graph and the initial messages
    val temporalStateGraph = init(weightedQueries, temporalGraph)
    val initMessages = new QueryStateMessages(
      List(QueryState.builder().withSeqNum(-1).withInitPathCost(Float.MaxValue).build()))

    // Extract the aggregation function from the queries
    val aggTests         = weightedQueries.sequence.map(q => q._2.aggTest)
    val aggCosts         = weightedQueries.sequence.map(q => q._2.aggCost)
    val aggIntervalTests = weightedQueries.sequence.map(q => q._2.aggIntervalTest)

    // Run Pregel
    val result = temporalStateGraph.pregel[QueryStateMessages](
      initMessages,
      Int.MaxValue,
      EdgeDirection.Out
    )(
      // Vertex Program
      (id, attr, msg) => {
        val (node, stateSequence) = attr
        val newStateSequence =
          stateSequence.map(
            state =>
              QueryState
                .builder()
                .fromState(state)
                .applyPathCostUpdate(msg.getMinCostBy(state.seqNum - 1))
                .build())
        (node, newStateSequence)
      },
      // Send Message
      triplet => {
        val srcStateSequence = triplet.srcAttr._2

        val messages = new QueryStateMessages(
          srcStateSequence
            .map(queryState => {
              val seqNum = queryState.seqNum
              QueryState
                .builder()
                .fromState(queryState)
                .applyWeightedPregelTriplet(triplet, aggTests(seqNum), aggIntervalTests(seqNum), aggCosts(seqNum))
                .build()
            })
            .filter(newState => newState.testSuccess)
        )

        Iterator.single((triplet.dstId, messages))
      },
      // Merge Message
      (a, b) => {
        a.merge(b)
      }
    )

    result
  }

  def init(weightedQueries: WeightedQueries, temporalGraph: TemporalGraph): TemporalPregelGraph = {
    // Extract the test, cost functions and empty states from the queries
    val nodeTests   = weightedQueries.sequence.map(q => SequencedQueries.extractConstQuery(q._1).nodeTest)
    val nodeCosts   = weightedQueries.sequence.map(q => SequencedQueries.extractConstQuery(q._1).nodeCost)
    val emptyStates = weightedQueries.createInitStates()

    // Map to temporal pregel graph
    temporalGraph.mapVertices((id, attr) => {
      val initStates =
        emptyStates.map(
          state =>
            QueryState
              .builder()
              .fromState(state)
              .applyNodeTest(attr, nodeTests(state.seqNum))
              .applyNodeCost(attr, nodeCosts(state.seqNum))
              .applyPathCostUpdate(if (state.seqNum == 0) nodeCosts(state.seqNum)(attr) else Float.MaxValue)
              .build())
      (attr, initStates)
    })
  }

}
