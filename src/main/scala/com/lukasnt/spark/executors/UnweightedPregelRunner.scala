package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{TemporalGraph, TemporalPregelGraph}
import com.lukasnt.spark.models.{QueryState, QueryStateMessages, SequencedQueries, UnweightedQueries}
import org.apache.spark.graphx.EdgeDirection

/**
  * Executes a temporal path query using Pregel.
  */
object UnweightedPregelRunner {

  /**
    * Runs the temporal path query on a temporal graph.
    * @param temporalGraph temporal graph
    * @return temporal graph with the updated PathQueryState for each vertex
    */
  def run(unweightedQueries: UnweightedQueries, temporalGraph: TemporalGraph): TemporalPregelGraph = {
    // Create the init states beforehand as TemporaryPathQuery is not serializable
    val initStates   = unweightedQueries.createInitStates()
    val initMessages = new QueryStateMessages(initStates)

    // Map to temporal pregel graph
    val temporalStateGraph = temporalGraph.mapVertices((id, attr) => (attr, initStates))

    // Extract the test functions from the queries
    val nodeTests = unweightedQueries.sequence.map(q => SequencedQueries.extractConstQuery(q._1).nodeTest)

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
                .applyNodeTest(node, nodeTests(state.seqNum))
                .build())
        (node, newStateSequence)
      },
      // Send Message
      triplet => {
        Iterator.empty
      },
      // Merge Message
      (a, b) => {
        a.merge(b)
      }
    )

    result
  }

}
