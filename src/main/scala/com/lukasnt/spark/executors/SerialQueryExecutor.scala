package com.lukasnt.spark.executors

import com.lukasnt.spark.experiments.Experiment
import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types._
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}
import org.apache.spark.graphx.{EdgeTriplet, VertexId}

class SerialQueryExecutor extends ParameterQueryExecutor {

  def execute(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): QueryResult = {
    val totalStartTime = System.currentTimeMillis()

    // Extract predicates as query is not serializable
    val sourcePredicate: AttrVertex => Boolean      = parameterQuery.sourcePredicate
    val destinationPredicate: AttrVertex => Boolean = parameterQuery.destinationPredicate

    // Collect source and destination vertices
    val sourceVertices: List[VertexId] = temporalGraph.vertices
      .filter(v => sourcePredicate(AttrVertex(v)))
      .map(_._1)
      .collect()
      .toList
    val destinationVertices: List[VertexId] = temporalGraph.vertices
      .filter(v => destinationPredicate(AttrVertex(v)))
      .map(_._1)
      .collect()
      .toList

    val pathEntries = sourceVertices
      .flatMap(srcId => runBfs(temporalGraph, parameterQuery, srcId, destinationVertices))
      .sortBy(_.weight)
      .take(parameterQuery.topK)

    val totalExecutionTime = System.currentTimeMillis() - totalStartTime
    println(s"Total execution time: $totalExecutionTime ms")

    Experiment.measureExecutionTime(
      subgraphPhaseTime = 0,
      weightMapPhaseTime = 0,
      pregelPhaseTime = 0,
      pathConstructionPhaseTime = 0,
      totalExecutionTime = totalExecutionTime
    )

    // For each source and destination vertex pair, run BFS
    // and collect the top-k paths
    QueryResult(
      queriedGraph = temporalGraph,
      pathEntries = pathEntries
    )
  }

  private def runBfs(graph: TemporalGraph,
                     query: ParameterQuery,
                     srcId: VertexId,
                     destinationIds: List[VertexId]): List[SerialQueryExecutor.PathEntry] = {
    var queue          = List[SerialQueryExecutor.PathEntry]()
    var paths          = List[SerialQueryExecutor.PathEntry]()
    val graphTriplets  = collectAllTriplets(graph)
    queue = initQueue(graphTriplets, query, srcId)

    // BFS
    while (queue.nonEmpty) {
      val current = queue.head
      queue = queue.tail
      val currentPath     = current.path
      val currentInterval = current.interval
      val currentWeight   = current.weight
      val currentEndNode  = currentPath.endNode
      val currentLength   = currentPath.length

      // Found path to destination
      if (destinationIds.contains(currentEndNode) && currentLength >= query.minLength && currentLength <= query.maxLength) {
        paths = paths :+ current
      }

      // Extend current path
      if (currentLength < query.maxLength) {
        val outEdges = outTriplets(graphTriplets, currentEndNode)
        val newPaths = outEdges
          .filter(
            edge => {
              query.temporalPathType.validEdgeInterval(currentInterval, edge.attr.interval) &&
              query.intermediatePredicate(AttrEdge(edge.srcId, edge.dstId, edge.attr))
            }
          )
          .map(edge => {
            val interval = query.temporalPathType.nextInterval(currentInterval, edge.attr.interval)
            val weight   = query.weightMap(AttrEdge(edge.srcId, edge.dstId, edge.attr))
            SerialQueryExecutor.PathEntry(currentPath :+ edge, interval, currentWeight + weight)
          })
        queue = queue ++ newPaths

        Experiment.measureCurrentExecutionMemory()
      }

    }
    paths
  }

  private def initQueue(graphTriplets: List[EdgeTriplet[Properties, Properties]],
                        query: ParameterQuery,
                        srcId: VertexId): List[SerialQueryExecutor.PathEntry] = {
    val outEdges = outTriplets(graphTriplets, srcId)
    outEdges.map(edge => {
      val interval = query.temporalPathType.initInterval(edge.attr.interval)
      val weight   = query.weightMap(AttrEdge(edge.srcId, edge.dstId, edge.attr))
      SerialQueryExecutor.PathEntry(TemporalPath(edge), interval, weight)
    })
  }

  private def outTriplets(graphTriplets: List[EdgeTriplet[Properties, Properties]],
                          vertexId: VertexId): List[EdgeTriplet[Properties, Properties]] = {
    graphTriplets.filter(_.srcId == vertexId)
  }

  private def collectAllTriplets(graph: TemporalGraph): List[EdgeTriplet[Properties, Properties]] = {
    graph.triplets.collect().toList
  }

}

object SerialQueryExecutor {

  def apply(): SerialQueryExecutor = new SerialQueryExecutor()

  case class PathEntry(path: TemporalPath, interval: Interval, weight: Float)

}
