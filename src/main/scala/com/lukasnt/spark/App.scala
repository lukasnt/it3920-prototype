package com.lukasnt.spark

import com.lukasnt.spark.examples.{SimpleCSVLoader, SimpleGraphX, SimpleSpark, SimpleTemporalGraph, SimpleTemporalInterval}

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    //SimpleSpark.run()
    //SimpleGraphX.run()
    SimpleTemporalInterval.run()
    SimpleTemporalGraph.run()
    SimpleCSVLoader.run()

    // Wait for user input
    System.in.read()
  }
}
