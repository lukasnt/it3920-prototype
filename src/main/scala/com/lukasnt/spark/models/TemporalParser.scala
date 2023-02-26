package com.lukasnt.spark.models

import java.time.temporal.Temporal

trait TemporalParser[T <: Temporal] {

  def parse(temporal: String): T

}
