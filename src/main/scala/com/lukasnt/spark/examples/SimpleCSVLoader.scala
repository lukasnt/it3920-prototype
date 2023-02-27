package com.lukasnt.spark.examples

import com.lukasnt.spark.io.{LocalCSVLoader, SNBLoader, TemporalParser}
import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.sql.SparkSession

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

object SimpleCSVLoader {

  /**
    * Simple Temporal graph CSV loader example
    */
  def run(): Unit = {
    val spark = SparkSession.builder.appName("Temporal Graph CSV Loader").getOrCreate()
    val sc    = spark.sparkContext

    val temporalParser = new TemporalParser[ZonedDateTime] {
      override def parse(temporal: String): ZonedDateTime =
        ZonedDateTime.parse(temporal, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    }

    val propertiesLoader = new LocalCSVLoader(temporalParser)
    val snbLoader = new SNBLoader("/sf0.003_raw", propertiesLoader)

    val graph: TemporalGraph[ZonedDateTime] = snbLoader.load(sc)

    println("print after load")
    graph.vertices.collect().foreach(println)
    //graph.edges.collect().foreach(println)
  }

}
