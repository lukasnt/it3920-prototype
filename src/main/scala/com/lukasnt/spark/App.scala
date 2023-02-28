package com.lukasnt.spark

import com.lukasnt.spark.examples.{SimpleCSVLoader, SimpleTemporalGraph, SimpleTemporalInterval}

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    SimpleTemporalInterval.run()
    SimpleTemporalGraph.run()
    SimpleCSVLoader.run()

    // Wait for user input
    System.in.read()
  }
}
