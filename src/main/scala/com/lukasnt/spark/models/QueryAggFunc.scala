package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.{Interval, Properties}

class QueryAggFunc(val aggTest: (Properties, Properties, Properties) => Boolean = (srcAttr, dstAttr, edgeAttr) => true,
                   val aggCost: (Float, Properties) => Float = (pathCost, edgeAttr) => pathCost + 1.0f,
                   val aggIntervalTest: (Interval, Interval) => Boolean = (pathInterval, edgeInterval) => true) {}
