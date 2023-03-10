package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

import java.time.ZonedDateTime

class ConstQuery(val costFunc: TemporalProperties[ZonedDateTime] => Float = _ => 0.0f,
                 val testFunc: TemporalProperties[ZonedDateTime] => Boolean = _ => true)
    extends PathQuery {}
