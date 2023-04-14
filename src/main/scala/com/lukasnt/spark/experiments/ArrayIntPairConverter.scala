package com.lukasnt.spark.experiments

import picocli.CommandLine.ITypeConverter

class ArrayIntPairConverter extends ITypeConverter[Array[(Int, Int)]] {

  override def convert(value: String): Array[(Int, Int)] = {
    value.split(" ").map(new IntPairConverter().convert)
  }

}
