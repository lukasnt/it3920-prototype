package com.lukasnt.spark.visualizers

import com.lukasnt.spark.models.TemporalProperties
import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import java.time.ZonedDateTime
import java.util.UUID

object HTMLGenerator {

  def generateGraphGrid(graphs: List[TemporalGraph],
                        maxColumns: Int = 10,
                        profile: TemporalGraphProfile = VisualizerProfiles.defaultProfile): String = {
    val rows      = if (graphs.length / maxColumns == 0) 1 else graphs.length / maxColumns
    val maxHeight = 100
    val height    = Math.min(maxHeight, profile.height / rows)
    val width     = profile.width / Math.min(maxColumns, Math.max(graphs.length, 1))

    val gridProfile = new TemporalGraphProfile(
      width = width,
      height = height,
      linkDistance = 50
    )
    graphs
      .map(g => generateContainedGraph(UUID.randomUUID(), g.vertices.collect(), g.edges.collect(), gridProfile))
      .mkString(
        s"""<div style="display: grid; grid-template-columns: repeat($maxColumns, 1fr); grid-template-rows: repeat($rows, 1fr);">""",
        "",
        "</div>")
  }

  def generateGraph(graph: TemporalGraph, profile: TemporalGraphProfile = VisualizerProfiles.defaultProfile): String = {
    val uuid     = java.util.UUID.randomUUID
    val vertices = graph.vertices.collect()
    val edges    = graph.edges.collect()
    generateContainedGraph[TemporalProperties[ZonedDateTime], TemporalProperties[ZonedDateTime]](uuid,
                                                                                                 vertices,
                                                                                                 edges,
                                                                                                 profile)
  }

  def generateGenericGraph[VD, ED](graph: Graph[VD, ED], profile: VisualizerProfile[VD, ED] = null): String = {
    val uuid     = java.util.UUID.randomUUID
    val vertices = graph.vertices.collect()
    val edges    = graph.edges.collect()

    if (profile == null) {
      val genericVertices = graph.vertices.map(v => (v._1, v._2.toString)).collect()
      val genericEdges    = graph.edges.map(e => new Edge(e.srcId, e.dstId, e.attr.toString)).collect()
      val genericProfile  = new GenericGraphProfile()
      val uuid            = java.util.UUID.randomUUID
      generateContainedGraph[String, String](uuid, genericVertices, genericEdges, genericProfile)
    } else {
      generateContainedGraph(uuid, vertices, edges, profile)
    }
  }

  private def generateContainedGraph[VD, ED](containerID: UUID,
                                             vertices: Array[(VertexId, VD)],
                                             edges: Array[Edge[ED]],
                                             profile: VisualizerProfile[VD, ED]): String = {
    s"""<div>
      ${generateDivContainer(containerID, profile.width, profile.height)}
      ${generateStyleSheet(profile)}
      $generateD3Import
      <script>
        (() => {
          ${generateNodesVar(vertices, profile.vertexNameFunc)}
          ${generateLinksVar(vertices, edges)}
          ${generateSVGVars(containerID, profile.width, profile.height, profile.nodeRadius)}
          ${generateD3LayoutForce(profile.linkDistance,
                                  profile.charge,
                                  profile.chargeDistance,
                                  profile.friction,
                                  profile.linkStrength)}
        })()
      </script>
    </div>
  """
  }

  private def generateDivContainer(containerElementID: UUID, width: Int, height: Int): String = {
    s"""<div id='a""" + containerElementID + s"""' style='width:${width}px; height:${height}px'></div>"""
  }

  private def generateStyleSheet[VD, ED](profile: VisualizerProfile[VD, ED]): String = {
    s"""<style>
       |  .node circle { fill: ${profile.nodeColor}; }
       |  .node text { font: ${profile.textSize}px ${profile.textFont}; text-anchor: middle; fill: ${profile.textColor}; }
       |  line.link { stroke: ${profile.linkColor}; stroke-width: ${profile.linkWidth}px; }
    </style>""".stripMargin
  }

  private final def generateD3Import: String = {
    """<script src="//d3js.org/d3.v3.min.js"></script>"""
  }

  private def generateLinksVar[VD, ED](vertices: Array[(VertexId, VD)], edges: Array[Edge[ED]]): String = {
    s"""var links = [""" +
      edges
        .map(e =>
          "{source:nodes[" + vertices.indexWhere(_._1 == e.srcId) + "]," +
            "target:nodes[" + vertices.indexWhere(_._1 == e.dstId) + "]}")
        .mkString(",") + """];"""
  }

  private def generateNodesVar[VD](
      vertices: Array[(VertexId, VD)],
      vertexNameFunc: ((VertexId, VD)) => String = (v: (VertexId, VD)) => v._1.toString): String = {
    s"""var nodes = [""" +
      vertices
        .map(v => s"{id:${v._1},name:${"\"" + vertexNameFunc(v) + "\""}}")
        .mkString(",") + """];"""
  }

  private def generateSVGVars(containerElementID: UUID, width: Int, height: Int, nodeRadius: Int): String = {
    s"""
      var width = $width, height = $height;
      var svg = d3.select("#a""" + containerElementID + s"""").append("svg").attr("width", width).attr("height", height);
      ${svgAppendNode(nodeRadius)}
      ${svgAppendLink(nodeRadius)}
    """
  }

  private def svgAppendNode(nodeRadius: Int): String = {
    s"""
      var node = svg.selectAll(".node").data(nodes);
      var nodeEnter = node.enter().append("g").attr("class", "node")
      nodeEnter.append("circle").attr("r", $nodeRadius);
      nodeEnter.append("text").attr("dy", "0.35em").text(function(d) { return d.name; });
    """
  }

  private def svgAppendLink(nodeRadius: Int): String = {
    s"""
      /*
      var marker = svg.append("defs").append("marker")
        .attr("id", "arrowhead")
        .attr("refX", ${nodeRadius - 3})
        .attr("refY", ${nodeRadius - 3})
        .attr("markerWidth", ${4})
        .attr("markerHeight", ${4})
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0,0 V 4 L4,2 Z")
        .attr("stroke", "black")
        .attr("fill", "black");
      */
      var link = svg.selectAll(".link").data(links);
      link.enter().insert("line", ".node").attr("class", "link").attr("marker-end", "url(#arrowhead)");
    """
  }

  private def generateD3LayoutForce(linkDistance: Int,
                                    charge: Int,
                                    chargeDistance: Int,
                                    friction: Double,
                                    linkStrength: Double): String = {
    s"""
       d3.layout.force()
          .linkDistance($linkDistance)
          .charge($charge)
          .chargeDistance($chargeDistance)
          .friction($friction)
          .linkStrength($linkStrength)
          .size([width, height])
          .on("tick", function() {
            link.attr("x1", function(d) { return d.source.x; })
                .attr("y1", function(d) { return d.source.y; })
                .attr("x2", function(d) { return d.target.x; })
                .attr("y2", function(d) { return d.target.y; });
            node.attr("transform", function(d) {
                return "translate(" + d.x + "," + d.y + ")";
            });
        }).nodes(nodes).links(links).start();
    """
  }

}
