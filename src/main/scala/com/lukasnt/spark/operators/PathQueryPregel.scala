package com.lukasnt.spark.operators

import com.lukasnt.spark.models.TemporalPathQuery
import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.graphx.EdgeDirection

import java.time.LocalDateTime

/**
  * Executes a temporal path query using Pregel.
  */
object PathQueryPregel {

  /**
    * Runs the temporal path query on a temporal graph.
    * @param temporalGraph temporal graph
    * @param temporalPathQuery temporal path query
    * @return temporal graph with the updated PathQueryState for each vertex
    */
  def run(temporalGraph: TemporalGraph[LocalDateTime],
          temporalPathQuery: TemporalPathQuery): TemporalGraph[LocalDateTime] = {
    temporalGraph.pregel[LocalDateTime](
      null,
      Int.MaxValue,
      EdgeDirection.Out
    )(
      // Vertex Program
      (id, attr, msg) => {},
      // Send Message
      triplet => {},
      // Merge Message
      (a, b) => {}
    )
  }

}
