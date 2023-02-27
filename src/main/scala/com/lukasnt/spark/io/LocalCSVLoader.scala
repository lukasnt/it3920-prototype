package com.lukasnt.spark.io

import com.lukasnt.spark.models.{TemporalInterval, TemporalProperties}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

import java.time.temporal.Temporal
import scala.collection.mutable.ListBuffer
import scala.io.Source

class LocalCSVLoader[T <: Temporal](val temporalParser: TemporalParser[T],
                                    val separator: Char = '|',
                                    val startDateColumn: String = "creationDate",
                                    val endDateColumn: String = "deletionDate")
    extends TemporalPropertiesLoader[T] {

  override def readVerticesFile(sc: SparkContext,
                                path: String,
                                label: String): RDD[(VertexId, TemporalProperties[T])] = {
    val dataRows: ListBuffer[(VertexId, TemporalProperties[T])] = ListBuffer()

    // Read the file
    val fileSource = Source.fromInputStream(getClass.getResourceAsStream(path))
    val lines      = fileSource.getLines()

    // Read the first line as header and get the column indexes
    val headerColumns        = lines.next().split(separator)
    val startDateColumnIndex = headerColumns.indexOf(startDateColumn)
    val endDateColumnIndex   = headerColumns.indexOf(endDateColumn)
    val vertexIdColumnIndex  = headerColumns.indexOf("id")

    // Read the rest of the file
    for (line <- lines) {
      val columns = line.split(separator)

      // Get all the other columns values as properties
      val properties =
        getPropertiesExcludingColumns(headerColumns,
                                      columns,
                                      Array(vertexIdColumnIndex, startDateColumnIndex, endDateColumnIndex))

      // Create a TemporalProperties object
      val temporalProperties = new TemporalProperties[T](
        getTemporalInterval(columns, startDateColumnIndex, endDateColumnIndex),
        label,
        properties
      )

      // Create a new Vertex and add it to the data rows
      val vertexId = columns(vertexIdColumnIndex).toLong
      val newRow   = (vertexId, temporalProperties)
      dataRows += newRow
    }

    fileSource.close()
    sc.parallelize(dataRows)
  }

  override def readEdgesFile(sc: SparkContext,
                             path: String,
                             label: String,
                             srcLabel: String,
                             dstLabel: String): RDD[Edge[TemporalProperties[T]]] = {
    val dataRows: ListBuffer[Edge[TemporalProperties[T]]] = ListBuffer()

    // Read the file
    val fileSource = Source.fromInputStream(getClass.getResourceAsStream(path))
    val lines      = fileSource.getLines()

    // Read the first line as header and get the column indexes
    val headerColumns        = lines.next().split(separator)
    val startDateColumnIndex = headerColumns.indexOf(startDateColumn)
    val endDateColumnIndex   = headerColumns.indexOf(endDateColumn)
    val srcIdColumnIndex     = headerColumns.indexOf(s"${srcLabel}Id")
    val dstIdColumnIndex     = headerColumns.indexOf(s"${dstLabel}Id")

    // Read the rest of the file
    for (line <- lines) {
      val columns = line.split(separator)

      // Get all the other columns values as properties
      val properties = getPropertiesExcludingColumns(
        headerColumns,
        columns,
        Array(startDateColumnIndex, endDateColumnIndex, srcIdColumnIndex, dstIdColumnIndex))

      // Create a TemporalProperties object
      val temporalProperties = new TemporalProperties[T](
        getTemporalInterval(columns, startDateColumnIndex, endDateColumnIndex),
        label,
        properties
      )

      // Create a new Edge and add it to the data rows
      val srcId  = columns(srcIdColumnIndex).toLong
      val dstId  = columns(dstIdColumnIndex).toLong
      val newRow = Edge(srcId, dstId, temporalProperties)
      dataRows += newRow
    }

    fileSource.close()
    sc.parallelize(dataRows)
  }

  private def getPropertiesExcludingColumns(headerColumns: Array[String],
                                            columnValues: Array[String],
                                            excludedColumns: Array[Int]): Map[String, String] = {
    columnValues.zipWithIndex
      .filter {
        case (_, index) =>
          !excludedColumns.contains(index)
      }
      .map {
        case (value, index) =>
          headerColumns(index) -> value
      }
      .toMap
  }

  private def getTemporalInterval(columns: Array[String],
                                  startDateColumnIndex: Int,
                                  endDateColumnIndex: Int): TemporalInterval[T] = {
    val startDate = temporalParser.parse(columns(startDateColumnIndex))
    val endDate   = temporalParser.parse(columns(endDateColumnIndex))

    new TemporalInterval[T](startDate, endDate)
  }

}
