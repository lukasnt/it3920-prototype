package com.lukasnt.spark.experiments

import picocli.CommandLine.ITypeConverter

class IntPairConverter extends ITypeConverter[(Int, Int)] {

  override def convert(value: String): (Int, Int) = {
    val split = value.split(",")
    (split(0).toInt, split(1).toInt)
  }

}

