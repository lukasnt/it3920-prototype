package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.util.PathWeightTable
import com.lukasnt.spark.visualizers.HTMLGenerator
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class QueryResult(val queriedGraph: TemporalGraph,
                  val pathTable: PathWeightTable = PathWeightTable(tableEntries = List(), -1)) {

  def asDataFrame(sqlContext: SQLContext): DataFrame = {
    val schema = StructType(
      List(
        StructField("#", DataTypes.StringType, nullable = false),
        StructField("weight", DataTypes.FloatType, nullable = false),
        StructField("startId", DataTypes.StringType, nullable = false),
        StructField("endId", DataTypes.StringType, nullable = false),
        StructField("startTime", DataTypes.StringType, nullable = false),
        StructField("endTime", DataTypes.StringType, nullable = false),
        StructField("path", DataTypes.StringType, nullable = false)
      )
    )
    val rows: List[Row] = pathTable.entries.zipWithIndex.map {
      case (entry: PathWeightTable.Entry, index: Int) =>
        Row(
          index.toString,
          entry.weight,
          entry.path.startNode.toString,
          entry.path.endNode.toString,
          entry.path.interval.startTime.toString,
          entry.path.interval.endTime.toString,
          entry.path.toString
        )
    }

    val result = sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(rows), schema)
    result
  }

  def asGraphGrid: String = {
    HTMLGenerator.generateGraphGrid(asGraphList)
  }

  def asGraphList: List[TemporalGraph] = {
    pathTable.entries.map(entry => entry.path.asTemporalGraph(queriedGraph))
  }

}
