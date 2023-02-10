package com.lukasnt.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
 * @author ${user.name}
 */
object App {
  
  def main(args: Array[String]): Unit = {
    runGraphX()

    // Wait for user input
    System.in.read()
  }

  /**
   * Simple Spark example
   */
  private def runSimpleApp(): Unit = {
    val logFile = "./spark/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Applictaion").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

  /**
   * Simple GraphX example
   */
  private def runGraphX(): Unit = {
    val spark = SparkSession.builder.appName("GraphX").getOrCreate()
    val sc = spark.sparkContext

    // Create Graph from RDD of vertices and edges
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Seq(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof"))
    ))
    val relationships = sc.parallelize(Seq(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi")
    ))
    val defaultUser = ("John Doe", "Missing")
    val graph = Graph(users, relationships, defaultUser)

    // Print the vertices
    graph.vertices.collect().foreach(println)

    spark.stop()
  }
}
