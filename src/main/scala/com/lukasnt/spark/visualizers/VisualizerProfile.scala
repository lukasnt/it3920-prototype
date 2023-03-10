package com.lukasnt.spark.visualizers

import org.apache.spark.graphx.VertexId

object VisualizerProfile {
  val defaultProfile: TemporalGraphProfile     = new TemporalGraphProfile()
  val defaultGenericProfile: GenericGraphProfile = new GenericGraphProfile()
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
