package com.lukasnt.spark

import com.lukasnt.spark.examples.{SimpleCSVLoader, SimplePathQuery, SimpleTemporalGraph, SimpleTemporalInterval}

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    SimplePathQuery.result()

    // Wait for user input
    System.in.read()
  }
}
