package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{AttrEdge, AttrVertex, Properties, TemporalGraph}
import com.lukasnt.spark.queries.ParameterQuery
import org.apache.spark.graphx.Graph

class ParameterSubgraph(parameterQuery: ParameterQuery) extends SubgraphExecutor[Properties, Properties] {

  val sourcePredicate: AttrVertex => Boolean      = parameterQuery.sourcePredicate
  val intermediatePredicate: AttrEdge => Boolean  = parameterQuery.intermediatePredicate
  val destinationPredicate: AttrVertex => Boolean = parameterQuery.destinationPredicate

  /**
    * The subgraph function that filters the graph to keep only the vertices and edges that are source or destination or pass the intermediate predicate.
    * In addition, maps the graph such that the Pregel phase can use the source and destination properties on vertices.
    * @param temporalGraph The graph to filter
    * @return The filtered graph
    */
  override def subgraph(temporalGraph: TemporalGraph): Graph[Properties, Properties] = {
    // Map the graph first to add source and destination properties such that the Pregel phase can use them
    // Then filter the graph to keep only the vertices that are source or destination or pass the intermediate predicate
    temporalGraph
      .mapVertices(
        (id, attr) =>
          new Properties(
            attr.interval,
            attr.typeLabel,
            attr.properties +
              ("source"     -> sourcePredicate(AttrVertex(id, attr)).toString,
              "destination" -> destinationPredicate(AttrVertex(id, attr)).toString)
        ))
      .subgraph(
        vpred = (id, attr) => attr.properties("source").toBoolean || attr.properties("destination").toBoolean,
        epred = edge => intermediatePredicate(AttrEdge(edge.srcId, edge.dstId, edge.attr))
      )
  }

}

object ParameterSubgraph {

  def apply(temporalGraph: TemporalGraph, parameterQuery: ParameterQuery): Graph[Properties, Properties] =
    new ParameterSubgraph(parameterQuery).subgraph(temporalGraph)

}
