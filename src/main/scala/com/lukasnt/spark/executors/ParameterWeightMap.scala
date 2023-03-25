package com.lukasnt.spark.executors
import com.lukasnt.spark.models.Types.{AttrEdge, Properties, TemporalGraph}
import com.lukasnt.spark.queries.ParameterQuery

class ParameterWeightMap(parameterQuery: ParameterQuery) extends WeightMapExecutor {

  val weightMapFunction: AttrEdge => Float = parameterQuery.weightMap

  override def weightMap(temporalGraph: TemporalGraph): TemporalGraph = {
    temporalGraph.mapEdges(
      edge =>
        new Properties(
          edge.attr.interval,
          edge.attr.typeLabel,
          edge.attr.properties + ("weight" -> weightMapFunction(AttrEdge(edge.srcId, edge.dstId, edge.attr)).toString))
    )
  }
}

object ParameterWeightMap {

  def apply(temporalGraph: TemporalGraph, parameterQuery: ParameterQuery): TemporalGraph =
    new ParameterWeightMap(parameterQuery).weightMap(temporalGraph)

}
