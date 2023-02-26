package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.models.{TemporalInterval, TemporalParser, TemporalProperties}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import java.time.temporal.Temporal
import scala.collection.mutable.ListBuffer
import scala.io.Source

class LocalCSVLoader[T <: Temporal](val labelFiles: Map[String, String],
                                    val temporalParser: TemporalParser[T],
                                    val separator: Char = '|',
                                    val startDateColumn: String = "creationDate",
                                    val endDateColumn: String = "deletionDate",
                                    val srcIdColumn: String = "srcId",
                                    val dstIdColumn: String = "dstId")
    extends TemporalGraphLoader[T] {

  override def load(sc: SparkContext): TemporalGraph[T] = {

    // Initialize the RDD of vertices and edges
    var vertices: RDD[(VertexId, TemporalProperties[T])] = sc.emptyRDD
    var edges: RDD[Edge[TemporalProperties[T]]]          = sc.emptyRDD

    for ((label, file) <- labelFiles) {
      // Read the file and add the edges to the RDD
      val edgeList = readEdgeLabelFile(label, file)
      val edgesRDD = sc.parallelize(edgeList)
      edges = edges.union(edgesRDD)
    }

    // Default vertex properties
    val defaultVertexProperties = new TemporalProperties[T](
      findLifetimeInterval(edges),
      "default",
      Map()
    )

    Graph.fromEdges(edges, defaultVertexProperties)
  }

  private def readEdgeLabelFile(label: String, path: String): List[Edge[TemporalProperties[T]]] = {
    val dataRows: ListBuffer[Edge[TemporalProperties[T]]] = ListBuffer()

    // Read the file
    val fileSource = Source.fromInputStream(getClass.getResourceAsStream(path))
    val lines      = fileSource.getLines()

    // Read the first line as header and get the column indexes
    val headerColumns        = lines.next().split(separator)
    val startDateColumnIndex = headerColumns.indexOf(startDateColumn)
    val endDateColumnIndex   = headerColumns.indexOf(endDateColumn)
    val srcIdColumnIndex     = headerColumns.indexOf(srcIdColumn)
    val dstIdColumnIndex     = headerColumns.indexOf(dstIdColumn)

    // Read the rest of the file
    for (line <- lines) {
      val columns = line.split(separator)

      // Get the standard columns
      val startDate = temporalParser.parse(columns(startDateColumnIndex))
      val endDate   = temporalParser.parse(columns(endDateColumnIndex))
      val srcId     = columns(srcIdColumnIndex).toLong
      val dstId     = columns(dstIdColumnIndex).toLong

      // Get all the other columns values as properties
      val properties = columns.zipWithIndex
        .filter {
          case (_, index) =>
            index != startDateColumnIndex && index != endDateColumnIndex && index != srcIdColumnIndex && index != dstIdColumnIndex
        }
        .map {
          case (value, index) =>
            headerColumns(index) -> value
        }
        .toMap

      // Create a TemporalProperties object and add it to the data rows
      val temporalProperties = new TemporalProperties[T](
        new TemporalInterval[T](startDate, endDate),
        label,
        properties
      )

      val newRow = Edge(srcId, dstId, temporalProperties)

      dataRows += newRow
    }

    fileSource.close()
    dataRows.toList
  }

  private def findLifetimeInterval(edges: RDD[Edge[TemporalProperties[T]]]): TemporalInterval[T] = {
    val minInterval: TemporalInterval[T] =
      edges
        .reduce((e1, e2) => {
          if (e1.attr.interval.before(e2.attr.interval)) e1 else e2
        })
        .attr
        .interval

    val maxInterval: TemporalInterval[T] = edges
      .reduce((e1, e2) => {
        if (e1.attr.interval.before(e2.attr.interval)) e2 else e1
      })
      .attr
      .interval

    new TemporalInterval[T](minInterval.startTime, maxInterval.endTime)
  }

}
