package com.lukasnt.spark

import com.lukasnt.spark.examples.{SimpleGraphX, SimpleSpark, SimpleTemporalGraph, SimpleTemporalInterval}

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {
    SimpleSpark.run()
    SimpleGraphX.run()
    SimpleTemporalInterval.run()
    SimpleTemporalGraph.run()

    // Wait for user input
    System.in.read()
  }
}
