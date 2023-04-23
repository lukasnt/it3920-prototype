package com.lukasnt.spark.experiments

import com.lukasnt.spark.io.{SparkObjectLoader, TemporalGraphLoader}

import java.time.ZonedDateTime

object GraphLoaders {

  def getByName(name: String, size: String, hdfsRootDir: String): TemporalGraphLoader[ZonedDateTime] = {
    SparkObjectLoader(s"$hdfsRootDir/preprocessed/$name/$size")
  }

}
