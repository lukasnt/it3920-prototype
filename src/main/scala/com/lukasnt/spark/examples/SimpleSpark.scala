package com.lukasnt.spark.examples

import org.apache.spark.sql.SparkSession

object SimpleSpark {

  /**
    * Simple Spark example
    */
  def run(): Unit = {
    val logFile = "./README.md" // Should be some file on your system
    val spark   = SparkSession.builder.appName("Simple Applictaion").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs   = logData.filter(line => line.contains("a")).count()
    val numBs   = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}
