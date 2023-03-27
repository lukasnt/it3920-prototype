package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.{AttrEdge, PregelVertex, Properties}
import com.lukasnt.spark.queries.{ParameterQuery, PathsWeightTable}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

class ParameterPathsConstruction(parameterQuery: ParameterQuery)
    extends PathsConstructionExecutor[PregelVertex, Properties] {

  val minLength: Int               = parameterQuery.minLength
  val maxLength: Int               = parameterQuery.maxLength
  val topK: Int                    = parameterQuery.topK
  val weightMap: AttrEdge => Float = parameterQuery.weightMap

  override def constructPaths(pregelGraph: Graph[PregelVertex, Properties]): RDD[TemporalPath] = {
    val destinationTriplets = pregelGraph.triplets
      .filter(triplet => triplet.dstAttr.constState.destination)

    /*
    destinationTriplets
      .collect()
      .foreach(triplet => println(s"Source: ${triplet.srcId}, Destination: ${triplet.dstId}"))
     */

    var currentPathsTable: PathsWeightTable = destinationTriplets
      .map(
        triplet =>
          PathsWeightTable(
            tableEntries = triplet.dstAttr.intervalsState.firstTable
              .filterByLengthRange(minLength, maxLength, topK)
              .entries
              .filter(entry => entry.parentId == triplet.srcId)
              .map(entry =>
                PathsWeightTable.Entry(
                  path = TemporalPath(Edge[Properties](entry.parentId, triplet.dstId, triplet.attr)),
                  remainingWeight = entry.weight - triplet.attr.properties("weight").toFloat,
                  remainingLength = entry.length - 1
              )),
            topK = topK
        ))
      .reduce((tableA, tableB) => tableA.mergeWithTable(tableB, topK))

    // While there are still paths to be extended
    while (currentPathsTable.entries.exists(_.remainingLength > 0)) {
      currentPathsTable.entries.foreach(println)

      // Retrieve the vertices that are part of the current paths
      val vertexSet = currentPathsTable.entries.map(entry => entry.path.startNode).toSet
      val vertexMap = pregelGraph.vertices.filter { case (id, _) => vertexSet.contains(id) }.collect().toMap

      // Extend the paths
      val currentTableEntries = currentPathsTable.entries.map { entry =>
        if (entry.remainingLength <= 1) {
          PathsWeightTable.Entry(entry.path, entry.remainingWeight, 0)
        }

        vertexMap(entry.path.startNode).intervalsState.firstTable.entries.foreach(println)
        println(s"entry.remainingWeight: ${entry.remainingWeight}")

        val lengthEntries = vertexMap(entry.path.startNode).intervalsState.firstTable
          .getEntriesByLength(entry.remainingLength)
          .sortBy(_.weight)

        val nextVertexEntry =
          if (lengthEntries.length > 1) {
            val first = lengthEntries.find(_.weight >= entry.remainingWeight)
            if (first.nonEmpty) first.get else lengthEntries.head
          } else
            lengthEntries.head

        val tripletWeights = pregelGraph.triplets
          .filter(triplet => triplet.srcId == nextVertexEntry.parentId && triplet.dstId == entry.path.startNode)
          .map(triplet => weightMap(AttrEdge(triplet)))
          .collect()

        tripletWeights.foreach(println)

        PathsWeightTable.Entry(
          path = TemporalPath(Edge[Properties](nextVertexEntry.parentId, entry.path.startNode, null)) + entry.path,
          remainingWeight = entry.remainingWeight - tripletWeights.head,
          remainingLength = entry.remainingLength - 1
        )
      }

      // Update the current paths table
      currentPathsTable = PathsWeightTable(
        tableEntries = currentTableEntries,
        topK = topK
      )
    }

    pregelGraph.vertices.sparkContext.parallelize(currentPathsTable.entries.map(_.path))
  }

}
