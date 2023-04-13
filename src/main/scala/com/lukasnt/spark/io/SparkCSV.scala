package com.lukasnt.spark.io

import com.lukasnt.spark.io.CSVUtils.CSVProperties
import com.lukasnt.spark.models.TemporalProperties
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import java.time.temporal.Temporal

class SparkCSV[T <: Temporal](val sqlContext: SQLContext, csvProperties: CSVProperties[T])
    extends TemporalPropertiesReader[T] {

  override def readVerticesFile(sc: SparkContext,
                                path: String,
                                label: String): RDD[(VertexId, TemporalProperties[T])] = {
    // As this class is not serializable we need to create a new variable reference to the csvProperties
    val csvProps: CSVProperties[T] = csvProperties

    sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", csvProps.separator.toString)
      .load(s"$path")
      .rdd
      .map(row => {
        val vertexIdColumnIndex = row.fieldIndex(csvProps.idColumn)
        val startDateIndex: Int = row.fieldIndex(csvProps.startDateColumn)
        val endDateIndex: Int   = row.fieldIndex(csvProps.endDateColumn)

        val arrayRow: Array[String]      = row.toSeq.toArray.map(_.toString)
        val headerColumns: Array[String] = row.schema.fieldNames

        val temporalProperties = new TemporalProperties[T](
          CSVUtils.getTemporalInterval(arrayRow, startDateIndex, endDateIndex, csvProps),
          label,
          CSVUtils.getValuesExcludingColumns(headerColumns,
                                             arrayRow,
                                             Array(vertexIdColumnIndex, startDateIndex, endDateIndex))
        )
        (arrayRow(vertexIdColumnIndex).toLong, temporalProperties)
      })

  }

  override def readEdgesFile(sc: SparkContext,
                             path: String,
                             label: String,
                             srcLabel: String,
                             dstLabel: String): RDD[Edge[TemporalProperties[T]]] = {
    val csvProps: CSVProperties[T] = csvProperties

    sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", csvProps.separator.toString)
      .load(s"$path")
      .rdd
      .map(row => {
        val startDateIndex: Int = row.fieldIndex(csvProps.startDateColumn)
        val endDateIndex: Int   = row.fieldIndex(csvProps.endDateColumn)
        val srcIdIndex: Int     = row.fieldIndex(s"${srcLabel}Id")
        val dstIdIndex: Int     = row.fieldIndex(s"${dstLabel}Id")

        val arrayRow: Array[String]      = row.toSeq.toArray.map(_.toString)
        val headerColumns: Array[String] = row.schema.fieldNames

        val temporalProperties = new TemporalProperties[T](
          CSVUtils.getTemporalInterval(arrayRow, startDateIndex, endDateIndex, csvProps),
          label,
          CSVUtils.getValuesExcludingColumns(headerColumns,
                                             arrayRow,
                                             Array(startDateIndex, endDateIndex, srcIdIndex, dstIdIndex))
        )
        val srcId = arrayRow(srcIdIndex).toLong
        val dstId = arrayRow(dstIdIndex).toLong
        Edge(srcId, dstId, temporalProperties)
      })
  }

}

object SparkCSV {

  def apply[T <: Temporal](sqlContext: SQLContext, csvProperties: CSVProperties[T]): SparkCSV[T] =
    new SparkCSV[T](sqlContext, csvProperties)

}
