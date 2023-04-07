package com.lukasnt.spark.examples

import com.lukasnt.spark.io.Loggers
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleGraphX {

  /**
    * Simple GraphX example
    */
  def run(): Unit = {
    val spark = SparkSession.builder.appName("GraphX Test").getOrCreate()
    val sc    = spark.sparkContext

    // Create Graph from RDD of vertices and edges
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
      Seq(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))
      ))
    val relationships = sc.parallelize(
      Seq(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")
      ))
    val defaultUser = ("John Doe", "Missing")
    val graph       = Graph(users, relationships, defaultUser)

    users.saveAsTextFile()

    // Print the vertices
    graph.vertices
      .collect()
      .foreach(v => {
        println(s"Vertex: ${v._1} - ${v._2}")
        Loggers.root.debug(s"Vertex: ${v._1} - ${v._2}")
      })

    // Run pregel test
    graph
      .pregel("", 10, EdgeDirection.Out)(
        vprog = (id, attr, msg) => (attr._1, msg),
        sendMsg = triplet => {
          println(s"triplet: $triplet")
          Iterator((triplet.dstId, triplet.srcAttr.toString() + triplet.attr))
        },
        mergeMsg = (a, b) => a + b
      )
      .vertices
      .collect()
      .foreach(v => {
        println(s"Vertex: ${v._1} - ${v._2}")
        Loggers.root.debug(s"Vertex: ${v._1} - ${v._2}")
      })
  }

}
