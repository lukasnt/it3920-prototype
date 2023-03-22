package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{SequencedPregelGraph, TemporalGraph}
import com.lukasnt.spark.queries.{ConstState, ConstStateMessages, SequencedQueries}
import org.apache.spark.graphx.EdgeDirection

object WeightedPregelRunner {

  def run(sequencedQueries: SequencedQueries, temporalGraph: TemporalGraph): SequencedPregelGraph = {

    // Initialize the temporal pregel graph and the initial messages
    val temporalStateGraph = initGraph(sequencedQueries, temporalGraph)
    val initialMessage     = initMessages(sequencedQueries)
    val maxPathLength      = sequencedQueries.sequence.length * 3

    //temporalStateGraph.vertices.collect().foreach(println)

    // Extract the aggregation function from the queries
    val aggTests         = sequencedQueries.sequence.map(q => q._2.aggTest)
    val aggCosts         = sequencedQueries.sequence.map(q => q._2.aggCost)
    val aggIntervalTests = sequencedQueries.sequence.map(q => q._2.aggIntervalTest)

    // Run Pregel
    val result = temporalStateGraph.pregel[ConstStateMessages](
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
              ConstState
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
      },
      // Merge Message
      mergeMsg = (a, b) => {
        a.merge(b)
      }
    )

    result
  }

  def initGraph(sequencedQueries: SequencedQueries, temporalGraph: TemporalGraph): SequencedPregelGraph = {
    // Extract the test, cost functions and empty states from the queries
    val nodeTests   = sequencedQueries.sequence.map(q => q._1.nodeTest)
    val nodeCosts   = sequencedQueries.sequence.map(q => q._1.nodeCost)
    val emptyStates = sequencedQueries.createInitStates()

    // Map to temporal pregel graph
    val initGraph = temporalGraph.mapVertices((id, attr) => {
      val initStates =
        emptyStates.map(
          state =>
            ConstState
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

  private def initMessages(weightedQueries: SequencedQueries): ConstStateMessages = {
    new ConstStateMessages(List(ConstState.builder().withSeqNum(-1).withInitPathCost(Float.MaxValue).build()))
  }

}
