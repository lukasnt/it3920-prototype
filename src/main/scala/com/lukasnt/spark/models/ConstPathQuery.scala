package com.lukasnt.spark.models

import java.time.ZonedDateTime

class ConstPathQuery(val costFunc: TemporalProperties[ZonedDateTime] => Float = _ => 0.0f,
                     val testFunc: TemporalProperties[ZonedDateTime] => Boolean = _ => true) {}
