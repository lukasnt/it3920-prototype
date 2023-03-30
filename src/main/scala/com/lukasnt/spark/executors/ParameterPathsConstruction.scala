package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.{AttrEdge, Interval, PregelVertex, Properties}
import com.lukasnt.spark.queries.{LengthWeightTable, ParameterQuery, PathWeightTable}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

class ParameterPathsConstruction(parameterQuery: ParameterQuery)
    extends PathsConstructionExecutor[PregelVertex, Properties] {

  val minLength: Int               = parameterQuery.minLength
  val maxLength: Int               = parameterQuery.maxLength
  val topK: Int                    = parameterQuery.topK
  val weightMap: AttrEdge => Float = parameterQuery.weightMap

  override def constructPaths(pregelGraph: Graph[PregelVertex, Properties]): List[TemporalPath] = {
    newConstructPaths(pregelGraph)
  }

  def newConstructPaths(pregelGraph: Graph[PregelVertex, Properties]): List[TemporalPath] = {
    var pathTable = createInitPathTable(findDestinationTriplets(pregelGraph), topK, minLength, maxLength)
    while (pathTable.entries.exists(entry => entry.remainingLength > 1)) {
      pathTable = extendPaths(pathTable, topK, pregelGraph)
    }
    pathTable.entries.foreach(println)
    pathTable.entries.map(entry => entry.path)
  }

  private def findDestinationTriplets(
      pregelGraph: Graph[PregelVertex, Properties]): RDD[EdgeTriplet[PregelVertex, Properties]] = {
    pregelGraph.triplets
      .filter(triplet => triplet.dstAttr.constState.destination)
  }

  private def createInitPathTable(triplets: RDD[EdgeTriplet[PregelVertex, Properties]],
                                  topK: Int,
                                  minLength: Int,
                                  maxLength: Int): PathWeightTable = {
    val result = triplets
      .map(triplet => (triplet.dstId, triplet.dstAttr.intervalsState.firstTable))
      .aggregate[PathWeightTable](PathWeightTable(List(), topK))(
        seqOp = (pathTable, stateTable) => {
          // Skip the table if the destination vertex is already in the path table
          if (pathTable.entries.exists(_.path.endNode == stateTable._1)) {
            pathTable
          } else {
            pathTable.mergeWithTable(createPathTable(stateTable._2, stateTable._1, topK, minLength, maxLength, null),
                                     topK)
          }
        },
        combOp = (tableA, tableB) => {
          // Filter out duplicate entries (same destination node)
          val filteredTableB =
            PathWeightTable(
              tableB.entries.filter(entry => !tableA.entries.exists(_.path.endNode == entry.path.endNode)),
              topK)
          tableA.mergeWithTable(filteredTableB, topK)
        }
      )
    result
  }

  private def createPathTable(table: LengthWeightTable,
                              vertex: VertexId,
                              topK: Int,
                              minLength: Int,
                              maxLength: Int,
                              interval: Interval): PathWeightTable = {
    val tableEntries = table
      .filterByLengthRange(minLength, maxLength, topK)
      .entries
      .map(entry => {
        PathWeightTable.Entry(path = TemporalPath(Edge[Properties](entry.parentId, vertex, null)),
                              remainingWeight = entry.weight,
                              remainingLength = entry.length)
      })
    PathWeightTable(tableEntries, topK)
  }

  private def extendPaths(pathsTable: PathWeightTable,
                          topK: Int,
                          pregelGraph: Graph[PregelVertex, Properties]): PathWeightTable = {
    // Retrieve the states of vertices that are start-nodes of the current paths
    val vertexSet = pathsTable.entries.map(entry => entry.path.startNode).toSet
    val vertexMap = pregelGraph.vertices.filter { case (id, _) => vertexSet.contains(id) }.collect().toMap

    // Group the paths by their start-node and remaining length
    val groupedEntries =
      pathsTable.entries.groupBy(entry => {
        val startEdge = entry.path.edgeSequence.head
        (startEdge.srcId, startEdge.dstId, entry.remainingLength)
      })

    val result = groupedEntries.values
      .flatMap(entries => {
        val nextEntries = findNextEntries(entries.head.path.startNode,
                                          entries.head.remainingLength - 1,
                                          null,
                                          entries.length,
                                          vertexMap)
        if (entries.head.remainingLength - 1 > 0) {
          // Find the next entry for this group and extend the paths
          // val nextEntry = findNextEntry(entries.head.path.startNode, entries.head.remainingLength - 1, null, vertexMap)
          pairwiseExtendPaths(
            pathTable = PathWeightTable(entries, entries.length),
            lengthWeightTable = nextEntries,
          ).entries
        } else entries
      })
      .toList
      .sortBy(_.remainingWeight)

    PathWeightTable(result, topK)
  }

  private def findNextEntries(parentVertex: VertexId,
                              remainingLength: Int,
                              interval: Interval,
                              groupLength: Int,
                              vertexMap: Map[VertexId, PregelVertex]): LengthWeightTable = {
    vertexMap(parentVertex).intervalsState.firstTable
      .filterByLength(remainingLength, groupLength)
  }

  private def pairwiseExtendPaths(pathTable: PathWeightTable, lengthWeightTable: LengthWeightTable): PathWeightTable = {
    PathWeightTable(
      tableEntries = pathTable.entries.zip(lengthWeightTable.entries).map {
        case (pathEntry, lengthWeightEntry) =>
          PathWeightTable.Entry(
            path = TemporalPath(Edge[Properties](lengthWeightEntry.parentId, pathEntry.path.startNode, null)) + pathEntry.path,
            remainingWeight = pathEntry.remainingWeight,
            remainingLength = lengthWeightEntry.length
          )
      },
      topK = pathTable.entries.length
    )
  }

  /*
  def legacyConstructPaths(pregelGraph: Graph[PregelVertex, Properties]): List[TemporalPath] = {
    val destinationTriplets = pregelGraph.triplets
      .filter(triplet => triplet.dstAttr.constState.destination)

    var currentPathsTable: PathWeightTable = destinationTriplets
      .map(
        triplet =>
          PathWeightTable(
            tableEntries = triplet.dstAttr.intervalsState.firstTable
              .filterByLengthRange(minLength, maxLength, topK)
              .entries
              .filter(entry => entry.parentId == triplet.srcId)
              .map(entry =>
                PathWeightTable.Entry(
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
          PathWeightTable.Entry(entry.path, entry.remainingWeight, 0)
        }

        //vertexMap(entry.path.startNode).intervalsState.firstTable.entries.foreach(println)
        //println(s"entry.remainingWeight: ${entry.remainingWeight}")

        val lengthFiltered = vertexMap(entry.path.startNode).intervalsState.firstTable
          .filterByLength(entry.remainingLength, topK)

        val nextVertexEntry =
          if (lengthFiltered.size > 1) {
            val first = lengthFiltered.entries.find(_.weight >= entry.remainingWeight)
            if (first.nonEmpty) first.get else lengthFiltered.minEntry.get
          } else
            lengthFiltered.minEntry.get

        val tripletWeights = pregelGraph.triplets
          .filter(triplet => triplet.srcId == nextVertexEntry.parentId && triplet.dstId == entry.path.startNode)
          .map(triplet => weightMap(AttrEdge(triplet)))
          .collect()

        //tripletWeights.foreach(println)

        PathWeightTable.Entry(
          path = TemporalPath(Edge[Properties](nextVertexEntry.parentId, entry.path.startNode, null)) + entry.path,
          remainingWeight = entry.remainingWeight - tripletWeights.head,
          remainingLength = entry.remainingLength - 1
        )
      }

      // Update the current paths table
      currentPathsTable = PathWeightTable(
        tableEntries = currentTableEntries,
        topK = topK
      )
    }

    currentPathsTable.entries.map(_.path)
  }

  private def findNextEntry(parentVertex: VertexId,
                            remainingLength: Int,
                            interval: Interval,
                            vertexMap: Map[VertexId, PregelVertex]): LengthWeightTable.Entry = {
    vertexMap(parentVertex).intervalsState.firstTable
      .filterByLength(remainingLength, topK)
      .minEntry
      .get
  }

  private def extendWithEntry(pathTable: PathWeightTable,
                              lengthWeightEntry: LengthWeightTable.Entry): PathWeightTable = {
    val newEntries = pathTable.entries.map(
      pathEntry =>
        PathWeightTable.Entry(
          path = TemporalPath(Edge[Properties](lengthWeightEntry.parentId, pathEntry.path.startNode, null)) + pathEntry.path,
          remainingWeight = lengthWeightEntry.weight,
          remainingLength = lengthWeightEntry.length
      ))
    PathWeightTable(newEntries, topK)
  }
 */

}
