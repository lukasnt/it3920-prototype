package com.lukasnt.spark.operators

import com.lukasnt.spark.examples.SimplePathQuery
import com.lukasnt.spark.models.PathQueryState
import com.lukasnt.spark.models.Types.{TemporalGraph, TemporalPregelGraph}
import org.apache.spark.graphx.EdgeDirection

import java.time.ZonedDateTime

/**
  * Executes a temporal path query using Pregel.
  */
object PathQueryPregel {

  val currentQuery = SimplePathQuery.exampleQuery()

  /**
    * Runs the temporal path query on a temporal graph.
    * @param temporalGraph temporal graph
    * @return temporal graph with the updated PathQueryState for each vertex
    */
  def run(temporalGraph: TemporalGraph[ZonedDateTime]): TemporalPregelGraph[ZonedDateTime] = {

    // Create the init states beforehand as TemporaryPathQuery is not serializable
    val initStates = currentQuery.createInitStates()

    // Map to temporal pregel graph
    val temporalStateGraph = temporalGraph.mapVertices((id, attr) => (attr, initStates))

    // Run Pregel
    val result = temporalStateGraph.pregel[List[PathQueryState]](
      initStates,
      Int.MaxValue,
      EdgeDirection.Out
    )(
      // Vertex Program
      (id, attr, msg) => {
        val (node, stateSequence) = attr
        val newStateSequence = stateSequence.map(state => {
          val testFunc = currentQuery.getQueryBySeqNum(state.seqNum).testFunc
          var newState = new PathQueryState(state.seqNum, state.next)
          newState.testSuccess = testFunc(node)
          newState
        })
        (node, newStateSequence)
      },
      // Send Message
      triplet => {
        Iterator.empty
      },
      // Merge Message
      (a, b) => {
        a
      }
    )

    //result.vertices.map(v => v._2._2).collect().foreach(println)

    result
  }

}
