package com.lukasnt.spark.experiments

import com.lukasnt.spark.io.{InteractionLoader, RecruitmentLoader, SNBLoader, TemporalGraphLoader}

import java.time.ZonedDateTime

object PreprocessLoaders {

  def getByName(name: String, rawLoader: TemporalGraphLoader[ZonedDateTime]): TemporalGraphLoader[ZonedDateTime] = {
    name match {
      case "interaction" => new InteractionLoader(rawLoader.asInstanceOf[SNBLoader])
      case "recruitment" => new RecruitmentLoader(rawLoader.asInstanceOf[SNBLoader])
    }
  }

}
