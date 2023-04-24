package com.lukasnt.spark.experiments

import com.lukasnt.spark.io._

import java.time.ZonedDateTime

object PreprocessLoaders {

  def getByName(name: String, rawLoader: TemporalGraphLoader[ZonedDateTime]): TemporalGraphLoader[ZonedDateTime] = {
    name match {
      case "raw"         => new RawLoader(rawLoader).asInstanceOf[TemporalGraphLoader[ZonedDateTime]]
      case "interaction" => new InteractionLoader(rawLoader).asInstanceOf[TemporalGraphLoader[ZonedDateTime]]
      case "recruitment" => new RecruitmentLoader(rawLoader).asInstanceOf[TemporalGraphLoader[ZonedDateTime]]
      case _             => new RawLoader(rawLoader).asInstanceOf[TemporalGraphLoader[ZonedDateTime]]
    }
  }

}
