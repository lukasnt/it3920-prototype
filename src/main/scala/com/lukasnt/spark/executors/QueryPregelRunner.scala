package com.lukasnt.spark.executors

import com.lukasnt.spark.examples.SimplePathQuery
import com.lukasnt.spark.models.Types.{TemporalGraph, TemporalPregelGraph}
import com.lukasnt.spark.models.{ConstQuery, QueryState}
import org.apache.spark.graphx.EdgeDirection

/**
  * Executes a temporal path query using Pregel.
  */
object QueryPregelRunner {

  // Global variable for the current query to be used under testing
  var currentQuery = SimplePathQuery.exampleQuery()

  /**
    * Runs the temporal path query on a temporal graph.
    * @param temporalGraph temporal graph
    * @return temporal graph with the updated PathQueryState for each vertex
    */
  def run(temporalGraph: TemporalGraph): TemporalPregelGraph = {
    // Create the init states beforehand as TemporaryPathQuery is not serializable
    val initStates = currentQuery.createInitStates()
    println(initStates.map(_.seqNum).mkString(", "))

    // Map to temporal pregel graph
    val temporalStateGraph = temporalGraph.mapVertices((id, attr) => (attr, initStates))

    // Run Pregel
    val result = temporalStateGraph.pregel[List[QueryState]](
      initStates,
      Int.MaxValue,
      EdgeDirection.Out
    )(
      // Vertex Program
      (id, attr, msg) => {
        val (node, stateSequence) = attr
        val newStateSequence = stateSequence.map(state =>
          currentQuery.getQueryBySeqNum(state.seqNum) match {
            case query: ConstQuery =>
              QueryStateMapper.mapConstQuery(query, state, node)
            case _ => state
        })
        (node, newStateSequence)
      },
      // Send Message
      triplet => {
        Iterator.empty
      },
      // Merge Message
      (a, b) => {
        a ++ b
      }
    )

    result
  }

}
