package com.lukasnt.spark.examples

import com.lukasnt.spark.models.TemporalProperties
import com.lukasnt.spark.models.Types.GenericTemporalGraph
import org.apache.spark.graphx.{EdgeDirection, VertexId}

import java.time.LocalDateTime

/**
  * Earliest Time Arrival algorithm.
  */
object SimpleETAAlgorithm {

  /**
    * Runs the Earliest Time Arrival algorithm on a temporal graph.
    * @param temporalGraph temporal graph
    * @param srcId source vertex id
    * @return temporal graph with the earliest time arrival for each vertex
    */
  def run(temporalGraph: GenericTemporalGraph[LocalDateTime], srcId: VertexId): GenericTemporalGraph[LocalDateTime] = {
    temporalGraph.pregel[LocalDateTime](
      LocalDateTime.MAX,
      Int.MaxValue,
      EdgeDirection.Out
    )(
      // Vertex Program
      (id, attr, msg) => {
        val current =
          LocalDateTime.parse(attr.properties.getOrElse("eta", LocalDateTime.MAX.toString))
        if (id == srcId) {
          new TemporalProperties[LocalDateTime](attr.interval,
                                                attr.typeLabel,
                                                attr.properties ++ Map("eta" -> LocalDateTime.MIN.toString))
        } else if (msg.isBefore(current)) {
          new TemporalProperties[LocalDateTime](attr.interval,
                                                attr.typeLabel,
                                                attr.properties ++ Map("eta" -> msg.toString))
        } else {
          attr
        }
      },
      // Send Message
      triplet => {
        val current = LocalDateTime.parse(triplet.srcAttr.properties.getOrElse("eta", LocalDateTime.MAX.toString))
        val next    = LocalDateTime.parse(triplet.dstAttr.properties.getOrElse("eta", LocalDateTime.MAX.toString))
        if (current.isBefore(next)) {
          Iterator((triplet.dstId, current))
        } else {
          Iterator.empty
        }
      },
      // Merge Message
      (a, b) => {
        if (a.isBefore(b)) a else b
      }
    )
  }

}
