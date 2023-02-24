package com.lukasnt.spark.examples

import com.lukasnt.spark.models.TemporalInterval
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

object SimpleTemporalInterval {

  /**
    * Simple LocalDateTimeInterval example
    */
  def run(): Unit = {
    val spark = SparkSession.builder.appName("GraphX").getOrCreate()
    val sc    = spark.sparkContext

    // Create RDD of LocalDateTimeInterval
    val test: RDD[TemporalInterval[LocalDateTime]] = sc.parallelize(
      Seq(
        new TemporalInterval(LocalDateTime.of(0, 1, 1, 0, 0), LocalDateTime.of(0, 1, 1, 0, 1)),
        new TemporalInterval(LocalDateTime.of(1, 1, 1, 0, 2), LocalDateTime.of(1, 1, 1, 0, 3))
      ))

    // Just print out as an example
    test.collect().foreach(println)
  }

}
