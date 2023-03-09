package com.lukasnt.spark.examples

import com.lukasnt.spark.models.TemporalProperties
import org.apache.spark.graphx.VertexId

import java.time.ZonedDateTime

object VisualizerProfile {
  val defaultProfile: TemporalGraphProfile     = new TemporalGraphProfile()
  val defaultGraphProfile: GenericGraphProfile = new GenericGraphProfile()
}

trait VisualizerProfile[VD, ED] {

  // Style settings
  def textFont: String  = "sans-serif"
  def textSize: Int     = 11
  def textColor: String = "black"
  def nodeRadius: Int   = 8
  def nodeColor: String = "lightgreen"
  def linkColor: String = "gray"
  def linkWidth: Double = 1.5
  // --------------------------------------------------------------------------------

  // Canvas Settings
  def width: Int  = 960
  def height: Int = 500

  // --------------------------------------------------------------------------------
  // Force Layout Settings
  def linkDistance: Int    = 200
  def charge: Int          = -100
  def chargeDistance: Int  = 300
  def friction: Double     = 0.25
  def linkStrength: Double = 0.5
  // --------------------------------------------------------------------------------

  // Vertex name function settings
  def displayId: Boolean            = false
  def displayInterval: Boolean      = false
  def displayFirstPropOnly: Boolean = true
  def propertiesKeySet: Set[String] = Set("firstName")
  // --------------------------------------------------------------------------------

  def vertexNameFunc(vertex: (VertexId, VD)): String
}

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

class GenericGraphProfile extends VisualizerProfile[String, String] {

  def vertexNameFunc(vertex: (VertexId, String)): String = {
    vertex._1.toString
  }

}
