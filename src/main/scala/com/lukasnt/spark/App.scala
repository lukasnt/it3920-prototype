package com.lukasnt.spark

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  
  def main(args: Array[String]): Unit = {
    val logFile = "./spark/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Applictaion").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
