package com.lukasnt.spark.examples

class VisualizerProfile(
    // Style settings
    var textFont: String = "sans-serif",
    var textSize: Int = 10,
    var textColor: String = "black",
    var nodeRadius: Int = 8,
    var nodeColor: String = "gray",
    var linkColor: String = "gray",
    var linkWidth: Double = 1.5,

    // Canvas Settings
    var width: Int = 960,
    var height: Int = 500,

    // Force Layout Settings
    var linkDistance: Int = 50,
    var charge: Int = -100,
    var chargeDistance: Int = 300,
    var friction: Double = 0.25,
    var linkStrength: Double = 0.5
) {}

object VisualizerProfile {
  val defaultProfile: VisualizerProfile = new VisualizerProfile()
  val greenProfile: VisualizerProfile = new VisualizerProfile(
    nodeColor = "green",
    linkColor = "green"
  )
}
