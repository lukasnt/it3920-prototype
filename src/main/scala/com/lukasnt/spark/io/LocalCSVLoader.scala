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
                                    val endDateColumn: String = "deletionDate",
                                    val srcIdColumn: String = "srcId",
                                    val dstIdColumn: String = "dstId")
    extends TemporalPropertiesLoader[T] {

  override def readVerticesFile(sc: SparkContext,
                                path: String,
                                label: String): RDD[(VertexId, TemporalProperties[T])] = {
    ???
  }

  override def readEdgesFile(sc: SparkContext, path: String, label: String): RDD[Edge[TemporalProperties[T]]] = {
    val dataRows: ListBuffer[Edge[TemporalProperties[T]]] = ListBuffer()

    // Read the file
    println(path)
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
    sc.parallelize(dataRows)
  }

}
