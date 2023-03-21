package com.lukasnt.spark.examples

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.visualizers.HTMLGenerator.{generateGenericGraph, generateGraph, generateGraphGrid}
import com.lukasnt.spark.visualizers.{TemporalGraphProfile, VisualizerProfile, VisualizerProfiles}
import org.apache.spark.graphx.Graph

object SimpleGraphVisualizer {

  def drawGraphGrid(graphs: List[TemporalGraph],
                    columns: Int = 10,
                    profile: TemporalGraphProfile = VisualizerProfiles.defaultProfile): Unit = {
    println(s"""%html ${generateGraphGrid(graphs, columns, profile)}""")
  }

  def drawGraph(graph: TemporalGraph, profile: TemporalGraphProfile = VisualizerProfiles.defaultProfile): Unit = {
    println(s"""%html ${generateGraph(graph, profile)}""")
  }

  def drawGenericGraph[VD, ED](graph: Graph[VD, ED], profile: VisualizerProfile[VD, ED] = null): Unit = {
    println(s"""%html ${generateGenericGraph(graph, profile)}""")
  }

}
