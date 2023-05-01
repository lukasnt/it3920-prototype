package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.{Properties, TemporalGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime

class SparkObjectLoader(val path: String) extends TemporalGraphLoader[ZonedDateTime] {

  override def load(sc: SparkContext): TemporalGraph = {
    val vertices: RDD[(VertexId, Properties)] = sc.objectFile[(VertexId, Properties)](s"$path/vertices")
    val edges: RDD[Edge[Properties]]          = sc.objectFile[Edge[Properties]](s"$path/edges")

    Graph[Properties, Properties](vertices, edges)
  }

}

object SparkObjectLoader {

  def load(path: String, sc: SparkContext): TemporalGraph = apply(path).load(sc)

  def apply(path: String): SparkObjectLoader = new SparkObjectLoader(path)

}
