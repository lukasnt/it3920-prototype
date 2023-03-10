package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.{Interval, Properties}

class QueryAggFunc(val aggTest: (Properties, Properties, Properties) => Boolean = (_, _, _) => true,
                   val aggCost: (Float, Float, Properties) => Float = (_, _, _) => 0.0f,
                   val aggIntervalTest: (Interval, Interval, Interval) => Boolean = (_, _, _) => true) {}
