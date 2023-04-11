package com.lukasnt.spark.io

import java.time.temporal.Temporal

trait TemporalParser[T <: Temporal] extends Serializable {

  def parse(temporal: String): T

}
