package com.lukasnt.spark.io

import com.lukasnt.spark.models.TemporalInterval

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal

object CSVUtils {

  def getValuesExcludingColumns(headerColumns: Array[String],
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

  def getTemporalInterval[T <: Temporal](columns: Array[String],
                                         startDateColumnIndex: Int,
                                         endDateColumnIndex: Int,
                                         csvProperties: CSVProperties[T]): TemporalInterval[T] = {
    val startDate = csvProperties.temporalParser.parse(columns(startDateColumnIndex))
    val endDate   = csvProperties.temporalParser.parse(columns(endDateColumnIndex))

    new TemporalInterval[T](startDate, endDate)
  }

  trait CSVProperties[T <: Temporal] extends Serializable {

    val separator: Char
    val idColumn: String
    val startDateColumn: String
    val endDateColumn: String
    val temporalParser: TemporalParser[T]

  }

  object SnbCSVProperties extends CSVProperties[ZonedDateTime] {

    override val separator: Char         = '|'
    override val idColumn: String        = "id"
    override val startDateColumn: String = "creationDate"
    override val endDateColumn: String   = "deletionDate"
    override val temporalParser: TemporalParser[ZonedDateTime] = new TemporalParser[ZonedDateTime] {
      override def parse(temporal: String): ZonedDateTime =
        ZonedDateTime.parse(temporal, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }

  }

}
