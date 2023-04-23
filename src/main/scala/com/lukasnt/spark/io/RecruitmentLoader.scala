package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.GenericTemporalGraph
import org.apache.spark.SparkContext

import java.time.ZonedDateTime

class RecruitmentLoader(val snbLoader: SNBLoader) extends TemporalGraphLoader[ZonedDateTime] {

  override def load(sc: SparkContext): GenericTemporalGraph[ZonedDateTime] = {
    ???
  }

}
