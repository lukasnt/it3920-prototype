package com.lukasnt.spark.visualizers

import org.apache.spark.graphx.VertexId

class GenericGraphProfile extends VisualizerProfile[String, String] {

  def vertexNameFunc(vertex: (VertexId, String)): String = {
    vertex._1.toString
  }

}
