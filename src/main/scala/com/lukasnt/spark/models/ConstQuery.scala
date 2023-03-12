package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.{PathQuery, Properties}

class ConstQuery(val nodeCost: Properties => Float = attr => 0.0f, val nodeTest: Properties => Boolean = attr => true)
    extends PathQuery {}
