package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.Properties

class ConstQuery(val nodeCost: Properties => Float = attr => 0.0f, val nodeTest: Properties => Boolean = attr => true)
    extends {}
