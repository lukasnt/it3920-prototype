package com.lukasnt.spark.io

import com.lukasnt.spark.models.TemporalProperties
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

import java.time.temporal.Temporal

trait TemporalPropertiesReader[T <: Temporal] {

  def readVerticesFile(sc: SparkContext, path: String, label: String): RDD[(VertexId, TemporalProperties[T])]

  def readEdgesFile(sc: SparkContext,
                    path: String,
                    label: String,
                    srcLabel: String,
                    dstLabel: String): RDD[Edge[TemporalProperties[T]]]

}
