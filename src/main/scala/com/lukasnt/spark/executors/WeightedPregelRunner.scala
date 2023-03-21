package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{TemporalGraph, TemporalPregelGraph}
import com.lukasnt.spark.models.{QueryState, QueryStateMessages, SequencedQueries, WeightedQueries}
import com.lukasnt.spark.utils.Loggers
import org.apache.spark.graphx.EdgeDirection

object WeightedPregelRunner {

  def run(weightedQueries: WeightedQueries, temporalGraph: TemporalGraph): TemporalPregelGraph = {

    // Initialize the temporal pregel graph and the initial messages
    val temporalStateGraph = initGraph(weightedQueries, temporalGraph)
    val initialMessage     = initMessages(weightedQueries)
    val maxPathLength      = weightedQueries.sequence.length * 3

    //temporalStateGraph.vertices.collect().foreach(println)

    // Extract the aggregation function from the queries
    val aggTests         = weightedQueries.sequence.map(q => q._2.aggTest)
    val aggCosts         = weightedQueries.sequence.map(q => q._2.aggCost)
    val aggIntervalTests = weightedQueries.sequence.map(q => q._2.aggIntervalTest)

    // Run Pregel
    val result = temporalStateGraph.pregel[QueryStateMessages](
      initialMsg = initialMessage,
      maxIterations = maxPathLength,
      activeDirection = EdgeDirection.Out
    )(
      // Vertex Program
      vprog = (id, attr, msg) => {

        //Loggers.default.debug(s"Vertex Program: $id, $attr, $msg")

        val (node, stateSequence) = attr
        val newStateSequence =
          stateSequence.map(
            state =>
              QueryState
                .builder()
                .fromState(state)
                .incSuperstep()
                .applyPathCostUpdate(msg.getMinCostBy(state.seqNum - 1))
                .build())
        (node, newStateSequence)
      },
      // Send Message
      sendMsg = triplet => {
        val srcStateSequence = triplet.srcAttr._2

        Loggers.default.debug(
          s"srcSuperstep: ${triplet.srcAttr._2.apply(2).superstep}, " +
            s"dstSuperStep: ${triplet.dstAttr._2.apply(2).superstep}")

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
          //.filter(newState => newState.testSuccess)
        )

        List((triplet.srcId, new QueryStateMessages(List.empty)), (triplet.dstId, messages)).toIterator
      },
      // Merge Message
      mergeMsg = (a, b) => {
        a.merge(b)
      }
    )

    result
  }

  def initGraph(weightedQueries: WeightedQueries, temporalGraph: TemporalGraph): TemporalPregelGraph = {
    // Extract the test, cost functions and empty states from the queries
    val nodeTests   = weightedQueries.sequence.map(q => SequencedQueries.extractConstQuery(q._1).nodeTest)
    val nodeCosts   = weightedQueries.sequence.map(q => SequencedQueries.extractConstQuery(q._1).nodeCost)
    val emptyStates = weightedQueries.createInitStates()

    // Map to temporal pregel graph
    val initGraph = temporalGraph.mapVertices((id, attr) => {
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

    //initGraph.vertices.collect().foreach(println)

    initGraph
  }

  private def initMessages(weightedQueries: WeightedQueries): QueryStateMessages = {
    new QueryStateMessages(List(QueryState.builder().withSeqNum(-1).withInitPathCost(Float.MaxValue).build()))
  }

}
