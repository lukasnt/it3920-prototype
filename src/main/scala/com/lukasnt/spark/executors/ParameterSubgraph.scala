package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{AttrEdge, AttrVertex, Properties, TemporalGraph}
import com.lukasnt.spark.queries.ParameterQuery
import org.apache.spark.graphx.Graph

class ParameterSubgraph(parameterQuery: ParameterQuery) extends SubgraphExecutor[Properties, Properties] {

  val sourcePredicate: AttrVertex => Boolean      = parameterQuery.sourcePredicate
  val intermediatePredicate: AttrEdge => Boolean  = parameterQuery.intermediatePredicate
  val destinationPredicate: AttrVertex => Boolean = parameterQuery.destinationPredicate

  override def subgraph(temporalGraph: TemporalGraph): Graph[Properties, Properties] = {
    temporalGraph
      .subgraph(
        vpred = (id, attr) => sourcePredicate(AttrVertex(id, attr)) || destinationPredicate(AttrVertex(id, attr)),
        epred = edge => intermediatePredicate(AttrEdge(edge.srcId, edge.dstId, edge.attr))
      )
  }

}

object ParameterSubgraph {

  def apply(temporalGraph: TemporalGraph, parameterQuery: ParameterQuery): Graph[Properties, Properties] =
    new ParameterSubgraph(parameterQuery).subgraph(temporalGraph)

}
