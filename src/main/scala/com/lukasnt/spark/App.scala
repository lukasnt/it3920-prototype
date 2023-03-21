package com.lukasnt.spark

import com.lukasnt.spark.examples.SimpleConstQueries

/**
  * @author ${user.name}
  */
object App {

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    SimpleConstQueries.result()

    // Wait for user input
    System.in.read()
  }

}
