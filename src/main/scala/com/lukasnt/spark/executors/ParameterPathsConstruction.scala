package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.{PregelVertex, Properties}
import com.lukasnt.spark.queries.ParameterQuery
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

class ParameterPathsConstruction(parameterQuery: ParameterQuery)
    extends PathsConstructionExecutor[PregelVertex, Properties] {

  val minLength: Int = parameterQuery.minLength
  val maxLength: Int = parameterQuery.maxLength

  override def constructPaths(pregelGraph: Graph[PregelVertex, Properties]): RDD[TemporalPath] = {
    val destinations = pregelGraph.vertices
      .filter { case (_, v) => v.constState.destination }
      .map { case (id, v) => (id, v.intervalsState.firstTable) }

    ???
  }

}
