package com.lukasnt.spark.io

import com.lukasnt.spark.models.TemporalProperties
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

import java.time.temporal.Temporal

class PartitionedLocalCSV[T <: Temporal](val singleLocalCSV: SingleLocalCSV[T]) extends TemporalPropertiesReader[T] {

  override def readVerticesFile(sc: SparkContext,
                                path: String,
                                label: String): RDD[(VertexId, TemporalProperties[T])] = {
    findFilePaths(path)
      .map(file => singleLocalCSV.readVerticesFile(sc, s"$path$file", label))
      .reduce(_ union _)
  }

  private def findFilePaths(rootPath: String) = {
    DirUtils
      .listFilesInsideJar(s"$rootPath")
      .map(_.substring(rootPath.length))
      .filter(name => name.startsWith("part-") && name.endsWith(".csv"))
  }

  override def readEdgesFile(sc: SparkContext,
                             path: String,
                             label: String,
                             srcLabel: String,
                             dstLabel: String): RDD[Edge[TemporalProperties[T]]] = {
    findFilePaths(path)
      .map(file => singleLocalCSV.readEdgesFile(sc, s"$path$file", label, srcLabel, dstLabel))
      .reduce((a, b) => a.union(b))
  }

}
