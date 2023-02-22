package com.lukasnt.spark.io

import com.lukasnt.spark.Types.TemporalGraph
import com.lukasnt.spark.{TemporalInterval, TemporalParser, TemporalProperties}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal
import scala.io.Source

class CsvTemporalGraphLoader(val temporalParser: TemporalParser =
                               new TemporalParser {
                                 override def parse[T <: Temporal](
                                     temporal: String): T =
                                   DateTimeFormatter.ISO_DATE_TIME
                                     .parse(temporal)
                                     .asInstanceOf[T]
                               },
                             val separator: String = "|",
                             val startDateColumn: String = "creationDate",
                             val endDateColumn: String = "endDate",
                             val srcIdColumn: String = "srcId",
                             val dstIdColumn: String = "dstId")
    extends TemporalGraphLoader {

  override def readEdgeListFiles[T <: Temporal](
      sc: SparkContext,
      labelFiles: Map[String, String]): TemporalGraph[T] = {

    // Initialize the RDD of vertices and edges
    var vertices: RDD[(VertexId, TemporalProperties[T])] = sc.emptyRDD
    var edges: RDD[Edge[TemporalProperties[T]]]          = sc.emptyRDD

    val temporalGraph: TemporalGraph[T] = Graph.apply(vertices, edges)

    for ((label, file) <- labelFiles) {
      val dataRows: Array[Edge[TemporalProperties[T]]] = Array.empty

      val fileSource = Source.fromFile(file)
      val lines      = fileSource.getLines()

      // Read first line as header line
      val header               = lines.next().split(separator)
      val startDateColumnIndex = header.indexOf(startDateColumn)
      val endDateColumnIndex   = header.indexOf(endDateColumn)
      val srcIdColumnIndex     = header.indexOf(srcIdColumn)
      val dstIdColumnIndex     = header.indexOf(dstIdColumn)

      // Read the rest of the file
      for (line <- lines) {
        val columns   = line.split(separator)
        val startDate = temporalParser.parse(columns(startDateColumnIndex))
        val endDate   = temporalParser.parse(columns(endDateColumnIndex))
        val srcId     = columns(srcIdColumnIndex).toLong
        val dstId     = columns(dstIdColumnIndex).toLong

        // Get all the other columns as properties
        val properties = header
          .zip(columns)
          .filterNot(column =>
            column._1 == startDateColumn || column._1 == endDateColumn)
          .toMap

        // Create a TemporalProperties object
        val temporalProperties = new TemporalProperties[T](
          new TemporalInterval[T](startDate, endDate),
          label,
          properties)

        // Add the temporal properties to the data rows
        dataRows :+ Edge(srcId, dstId, temporalProperties)
      }

      edges = edges.union(sc.parallelize(dataRows))

      fileSource.close()
    }

    Graph.fromEdges(edges, null)
  }

}
