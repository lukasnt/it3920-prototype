package com.lukasnt.spark.experiments

import com.lukasnt.spark.io.{SNBLoader, SparkObjectLoader, TemporalGraphLoader}
import org.apache.spark.sql.SparkSession

import java.time.ZonedDateTime

object GraphLoaders {

  def getByName(name: String,
                size: String,
                spark: SparkSession,
                hdfsRootDir: String,
                rawSNB: Boolean = false,
                rawCitiBike: Boolean = false): TemporalGraphLoader[ZonedDateTime] = {
    if (rawSNB) {
      SNBLoader.getByName(size, spark.sqlContext, hdfsRootDir, fullGraph = true)
    } else if (rawCitiBike) {
      SparkObjectLoader(s"$hdfsRootDir/preprocessed/$name/$size")
    } else {
      SparkObjectLoader(s"$hdfsRootDir/preprocessed/$name/$size")
    }

  }

}
