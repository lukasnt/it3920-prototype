package com.lukasnt.spark.models

import java.time.ZonedDateTime

class QueryAggFunc(val aggTest: (TemporalProperties[ZonedDateTime],
                                TemporalProperties[ZonedDateTime],
                                TemporalProperties[ZonedDateTime]) => Boolean = (_, _, _) => true,
                   val aggCost: (Float, Float, TemporalProperties[ZonedDateTime]) => Float = (_, _, _) => 0.0f,
                   val aggIntervalTest: (TemporalInterval[ZonedDateTime],
                                        TemporalInterval[ZonedDateTime],
                                        TemporalInterval[ZonedDateTime]) => Boolean = (_, _, _) => true) {}
