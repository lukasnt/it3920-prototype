package com.lukasnt.spark.examples

import com.lukasnt.spark.io.CSVUtils.SnbCSVProperties
import com.lukasnt.spark.io.{PartitionedLocalCSV, SNBLoader, SingleLocalCSV, SparkCSV}
import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.sql.SparkSession

object SimpleSNBLoader {

  def loadLocal(): TemporalGraph = {
    val spark = SparkSession.builder.appName("Temporal Graph CSV Loader").getOrCreate()
    val sc    = spark.sparkContext

    val partitionedFileReader = PartitionedLocalCSV(SingleLocalCSV(SnbCSVProperties))
    val snbLoader             = SNBLoader("/sf0_003-raw", partitionedFileReader)
    snbLoader.load(sc)
  }

  def load(): TemporalGraph = {
    val spark = SparkSession.builder.appName("Temporal Graph CSV Loader").getOrCreate()
    val sc    = spark.sparkContext

    val hdfsRootDir: String = System.getenv("HDFS_ROOT_DIR")

    val sparkCSVReader = SparkCSV(spark.sqlContext, SnbCSVProperties)
    val snbLoader      = SNBLoader(s"$hdfsRootDir/sf1-raw", sparkCSVReader)
    snbLoader.load(sc)
  }

}
