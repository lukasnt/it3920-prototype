package com.lukasnt.spark

import java.time.temporal.Temporal

trait TemporalParser {

  def parse[T <: Temporal](temporal: String): T

}
