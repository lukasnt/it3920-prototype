package com.lukasnt.spark

import com.lukasnt.spark.Types.TemporalGraph
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {
    runSimpleApp()
    runGraphX()
    runLocalDateTimeInterval()
    runTemporalGraphTest()

    // Wait for user input
    System.in.read()
  }

  /**
    * Simple Spark example
    */
  private def runSimpleApp(): Unit = {
    val logFile = "./spark/README.md" // Should be some file on your system
    val spark   = SparkSession.builder.appName("Simple Applictaion").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs   = logData.filter(line => line.contains("a")).count()
    val numBs   = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

  /**
    * Simple GraphX example
    */
  private def runGraphX(): Unit = {
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

  /**
    * Simple LocalDateTimeInterval example
    */
  private def runLocalDateTimeInterval(): Unit = {
    val spark = SparkSession.builder.appName("GraphX").getOrCreate()
    val sc    = spark.sparkContext

    // Create RDD of LocalDateTimeInterval
    val test: RDD[TemporalInterval[LocalDateTime]] = sc.parallelize(
      Seq(
        new TemporalInterval(LocalDateTime.of(0, 1, 1, 0, 0),
                             LocalDateTime.of(0, 1, 1, 0, 1)),
        new TemporalInterval(LocalDateTime.of(1, 1, 1, 0, 2),
                             LocalDateTime.of(1, 1, 1, 0, 3))
      ))

    // Just print out as an example
    test.collect().foreach(println)
  }

  /**
    * Simple TemporalGraphTest example
    */
  private def runTemporalGraphTest(): Unit = {
    val spark =
      SparkSession.builder.appName("Simple Temporal Graph Test").getOrCreate()
    val sc = spark.sparkContext

    val edges: RDD[Edge[TemporalProperties[LocalDateTime]]] = sc.parallelize(
      Seq(
        new Edge(1,
                 2,
                 new TemporalProperties(
                   new TemporalInterval(
                     LocalDateTime.of(1, 1, 1, 1, 1),
                     LocalDateTime.of(2, 2, 2, 2, 2)
                   ),
                   "TestLabel",
                   Map("key" -> "value")
                 )),
        new Edge(2,
                 3,
                 new TemporalProperties(
                   new TemporalInterval(
                     LocalDateTime.of(1, 1, 1, 1, 1),
                     LocalDateTime.of(2, 2, 2, 2, 2)
                   ),
                   "TestLabel",
                   Map("key" -> "value")
                 ))
      ))

    val graph: TemporalGraph[LocalDateTime] = Graph.fromEdges(
      edges,
      new TemporalProperties(
        new TemporalInterval(LocalDateTime.of(0, 1, 1, 1, 0),
                             LocalDateTime.of(2, 2, 2, 2, 2)),
        "TestLabel",
        Map("key" -> "value")))

    graph.edges.foreach(println)
  }
}
