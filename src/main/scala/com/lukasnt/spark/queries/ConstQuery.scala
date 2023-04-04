package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.{Interval, Properties}

class ConstQuery(val nodeCost: Properties => Float = attr => 0.0f, val nodeTest: Properties => Boolean = attr => true)
    extends {}

object ConstQuery {

  class AggFunc(val aggTest: (Properties, Properties, Properties) => Boolean = (srcAttr, dstAttr, edgeAttr) => true,
                val aggCost: (Float, Properties) => Float = (pathCost, edgeAttr) => pathCost + 1.0f,
                val aggIntervalTest: (Interval, Interval) => Boolean = (pathInterval, edgeInterval) => true) {}

}
