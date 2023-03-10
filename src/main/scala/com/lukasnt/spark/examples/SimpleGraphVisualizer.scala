package com.lukasnt.spark.examples

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.visualizers.HTMLGenerator.{generateGenericGraph, generateGraph, generateGraphGrid}
import com.lukasnt.spark.visualizers.{TemporalGraphProfile, VisualizerProfile}
import org.apache.spark.graphx.Graph

import java.time.ZonedDateTime

object SimpleGraphVisualizer {

  def drawGraphGrid(graphs: List[TemporalGraph[ZonedDateTime]],
                    columns: Int = 10,
                    profile: TemporalGraphProfile = VisualizerProfile.defaultProfile): Unit = {

    println(s"""%html ${generateGraphGrid(graphs, columns, profile)}""")
  }

  def drawGraph(graph: TemporalGraph[ZonedDateTime],
                profile: TemporalGraphProfile = VisualizerProfile.defaultProfile): Unit = {
    println(s"""%html ${generateGraph(graph, profile)}""")
  }

  def drawGenericGraph[VD, ED](graph: Graph[VD, ED], profile: VisualizerProfile[VD, ED] = null): Unit = {
    println(s"""%html ${generateGenericGraph(graph, profile)}""")
  }

}
