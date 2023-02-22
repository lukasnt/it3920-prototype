package com.lukasnt.spark.operators

import com.lukasnt.spark.models.TemporalProperties
import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.graphx.{EdgeDirection, VertexId}

import java.time.LocalDateTime

/**
  * Earliest Time Arrival algorithm.
  */
object EarliestTimeArrival {

  /**
    * Runs the Earliest Time Arrival algorithm on a temporal graph.
    * @param temporalGraph temporal graph
    * @param srcId source vertex id
    * @return temporal graph with the earliest time arrival for each vertex
    */
  def run(temporalGraph: TemporalGraph[LocalDateTime],
          srcId: VertexId): TemporalGraph[LocalDateTime] = {
    temporalGraph.pregel[LocalDateTime](
      LocalDateTime.MAX,
      Int.MaxValue,
      EdgeDirection.Out
    )(
      // Vertex Program
      (id, attr, msg) => {
        val current =
          new LocalDateTime(attr.properties.getOrElse("eta", LocalDateTime.MAX))
        if (id == srcId) {
          new TemporalProperties[LocalDateTime](
            attr.interval,
            attr.typeLabel,
            attr.properties ++ Map("eta" -> LocalDateTime.MIN.toString))
        } else if (msg.isBefore(current)) {
          new TemporalProperties[LocalDateTime](
            attr.interval,
            attr.typeLabel,
            attr.properties ++ Map("eta" -> msg.toString))
        } else {
          attr
        }
      },
      // Send Message
      triplet => {
        val current = new LocalDateTime(
          triplet.srcAttr.properties.getOrElse("eta", LocalDateTime.MAX))
        val next = new LocalDateTime(
          triplet.dstAttr.properties.getOrElse("eta", LocalDateTime.MAX))
        if (current.isBefore(next)) {
          Iterator((triplet.dstId, current))
        } else {
          Iterator.empty
        }
      },
      // Merge Message
      (a, b) => {
        if (a.isBefore(b)) {
          a
        } else {
          b
        }
      }
    )
  }

}
