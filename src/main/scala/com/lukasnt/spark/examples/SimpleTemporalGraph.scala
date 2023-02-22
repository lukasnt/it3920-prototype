package com.lukasnt.spark.examples

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.models.{TemporalInterval, TemporalProperties}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

object SimpleTemporalGraph {

  /**
    * Simple TemporalGraphTest example
    */
  def run(): Unit = {
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
