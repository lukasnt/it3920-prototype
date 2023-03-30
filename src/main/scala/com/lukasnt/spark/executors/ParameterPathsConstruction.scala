package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.{AttrEdge, Interval, PregelVertex, Properties}
import com.lukasnt.spark.queries.{LengthWeightTable, ParameterQuery, PathWeightTable}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

class ParameterPathsConstruction(parameterQuery: ParameterQuery)
    extends PathsConstructionExecutor[PregelVertex, Properties] {

  val minLength: Int                                     = parameterQuery.minLength
  val maxLength: Int                                     = parameterQuery.maxLength
  val topK: Int                                          = parameterQuery.topK
  val weightMap: AttrEdge => Float                       = parameterQuery.weightMap
  val intervalFunction: (Interval, Interval) => Interval = parameterQuery.intervalRelation

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
        PathWeightTable.Entry(
          interval = interval,
          remainingLength = entry.length,
          weight = entry.weight,
          path = TemporalPath(
            Edge[Properties](srcId = entry.parentId, dstId = vertex, attr = new Properties(interval, null, null)))
        )
      })
    PathWeightTable(tableEntries, topK)
  }

  private def extendPaths(pathsTable: PathWeightTable,
                          topK: Int,
                          pregelGraph: Graph[PregelVertex, Properties]): PathWeightTable = {
    // Retrieve the states of vertices that are start-nodes of the current paths
    val vertexSet = pathsTable.entries.map(entry => entry.path.startNode).toSet
    val vertexMap = pregelGraph.vertices.filter { case (id, _) => vertexSet.contains(id) }.collect().toMap

    // Group the paths by their first edge and remaining length
    val groupedEntries =
      pathsTable.entries.groupBy(entry => {
        val startEdge = entry.path.edgeSequence.head
        (startEdge.srcId, startEdge.dstId, startEdge.attr.interval, entry.remainingLength)
      })

    groupedEntries
      .map {
        case ((srcId, _, interval, remainingLength), entries) =>
          if (remainingLength - 1 > 0) {
            // Find the next entries for this group and pairwise extend the paths
            pairwiseExtendPaths(
              pathTable = PathWeightTable(entries, entries.length),
              lengthWeightTable = findNextEntries(srcId, remainingLength - 1, interval, entries.length, vertexMap),
              interval = interval
            )
          } else PathWeightTable(entries, entries.length)
      }
      .reduce((tableA, tableB) => tableA.mergeWithTable(tableB, topK))
  }

  private def findNextEntries(parentVertex: VertexId,
                              remainingLength: Int,
                              interval: Interval,
                              groupLength: Int,
                              vertexMap: Map[VertexId, PregelVertex]): LengthWeightTable = {
    vertexMap(parentVertex).intervalsState
      .lengthFilteredTable(remainingLength, groupLength)
    //.intervalFunctionFilteredTable(intervalFunction, interval, -1)
    //.filterByLength(remainingLength, groupLength)
  }

  private def pairwiseExtendPaths(pathTable: PathWeightTable,
                                  lengthWeightTable: LengthWeightTable,
                                  interval: Interval): PathWeightTable = {
    PathWeightTable(
      tableEntries = pathTable.entries.zip(lengthWeightTable.entries).map {
        case (pathEntry, lengthWeightEntry) =>
          PathWeightTable.Entry(
            interval = interval,
            remainingLength = lengthWeightEntry.length,
            weight = pathEntry.weight,
            path = TemporalPath(
              Edge[Properties](lengthWeightEntry.parentId,
                               pathEntry.path.startNode,
                               new Properties(interval, null, null))) + pathEntry.path
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
