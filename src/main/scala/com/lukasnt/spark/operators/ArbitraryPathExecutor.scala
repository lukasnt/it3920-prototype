package com.lukasnt.spark.operators

import com.lukasnt.spark.models.{ArbitraryPathQuery, PathQueryState, TemporalProperties}

import java.time.ZonedDateTime

object ArbitraryPathExecutor {
  def execute(arbitraryPathQuery: ArbitraryPathQuery,
              state: PathQueryState,
              node: TemporalProperties[ZonedDateTime]): PathQueryState = ???
}
