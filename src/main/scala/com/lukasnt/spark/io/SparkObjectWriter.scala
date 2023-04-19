package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.TemporalGraph

import java.io.IOException
import java.time.ZonedDateTime

class SparkObjectWriter extends TemporalGraphWriter[ZonedDateTime] {

  override def write(graph: TemporalGraph, path: String): Unit = {
    try {
      graph.vertices.saveAsObjectFile(s"$path/vertices")
      graph.edges.saveAsObjectFile(s"$path/edges")
    } catch {
      case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
        println(s"SparkObjectWriter: File already exists at $path, skipping...")
      case _: org.apache.hadoop.mapred.FileAlreadyExistsException =>
        println(s"SparkObjectWriter: File already exists at $path, skipping...")
      case _: IOException =>
        println(s"SparkObjectWriter: Could not write to file at $path, skipping...")
    }
  }
}

object SparkObjectWriter {

  def write(graph: TemporalGraph, path: String): Unit = apply().write(graph, path)

  def apply(): SparkObjectWriter = new SparkObjectWriter()

}
