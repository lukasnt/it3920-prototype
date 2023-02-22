package com.lukasnt.spark.examples

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleGraphX {

  /**
    * Simple GraphX example
    */
  def run(): Unit = {
    val spark = SparkSession.builder.appName("GraphX").getOrCreate()
    val sc    = spark.sparkContext
    sc.setLogLevel("ERROR")

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

    // Print the vertices
    graph.vertices.collect().foreach(println)
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
      .foreach(println)

    spark.stop()
  }

}
