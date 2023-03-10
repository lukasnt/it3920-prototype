package com.lukasnt.spark.visualizers

import com.lukasnt.spark.models.TemporalProperties
import org.apache.spark.graphx.VertexId

import java.time.ZonedDateTime

class TemporalGraphProfile(override val textFont: String = "sans-serif",
                           override val textSize: Int = 11,
                           override val textColor: String = "black",
                           override val nodeRadius: Int = 8,
                           override val nodeColor: String = "lightgreen",
                           override val linkColor: String = "gray",
                           override val linkWidth: Double = 1.5,
                           override val width: Int = 960,
                           override val height: Int = 500,
                           override val linkDistance: Int = 350,
                           override val charge: Int = -100,
                           override val chargeDistance: Int = 300,
                           override val friction: Double = 0.25,
                           override val linkStrength: Double = 0.5,
                           override val displayId: Boolean = false,
                           override val displayInterval: Boolean = false,
                           override val displayFirstPropOnly: Boolean = true,
                           override val propertiesKeySet: Set[String] = Set("firstName"))
    extends VisualizerProfile[TemporalProperties[ZonedDateTime], TemporalProperties[ZonedDateTime]] {

  def vertexNameFunc(vertex: (VertexId, TemporalProperties[ZonedDateTime])): String = {
    val idString = if (displayId) vertex._1 else ""
    val intervalString = if (displayInterval) {
      s"(${vertex._2.interval.startTime}, ${vertex._2.interval.endTime})"
    } else ""
    val propertiesString = if (!displayFirstPropOnly) {
      s"(${vertex._2.properties.filterKeys(p => propertiesKeySet.contains(p)).mkString(", ")})"
    } else {
      s"${vertex._2.properties.applyOrElse(propertiesKeySet.head, (_: String) => "")}"
    }
    List(idString, intervalString, propertiesString).filter(_ != "").mkString(", ")
  }

}
