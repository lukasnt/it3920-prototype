package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.{Properties, TemporalGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexRDD

import java.time.ZonedDateTime

class InteractionLoader(val snbLoader: SNBLoader) extends TemporalGraphLoader[ZonedDateTime] {

  override def load(sc: SparkContext): TemporalGraph = {
    val rawGraph: TemporalGraph = snbLoader.load(sc)

    rawGraph.mapTriplets(triplet => {
      val srcComments: VertexRDD[Properties] = rawGraph.vertices.filter {
        case (_, attr) => attr.typeLabel == "Comment" && attr.properties("CreatorPersonID").toInt == triplet.srcId
      }
      val dstComments: VertexRDD[Properties] = rawGraph.vertices.filter {
        case (_, attr) => attr.typeLabel == "Comment" && attr.properties("CreatorPersonID").toInt == triplet.dstId
      }
      ???
    })
  }
}
