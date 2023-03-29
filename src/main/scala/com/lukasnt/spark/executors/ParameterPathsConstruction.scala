package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.{AttrEdge, Interval, PregelVertex, Properties}
import com.lukasnt.spark.queries.{LengthWeightTable, ParameterQuery, PathWeightTable}
import com.lukasnt.spark.utils.Loggers
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

    //legacyConstructPaths(pregelGraph)
  }

  def newConstructPaths(pregelGraph: Graph[PregelVertex, Properties]): List[TemporalPath] = {
    val triplets = findDestinationTriplets(pregelGraph).collect()
    /*triplets.foreach(triplet => {
      println(triplet.dstId)
      println(triplet.dstAttr.intervalsState.firstTable)
    })*/
    var pathTable = createInitPathTable(findDestinationTriplets(pregelGraph), topK, minLength, maxLength)
    while (pathTable.entries.exists(entry => entry.remainingLength > 1)) {
      pathTable = extendPaths(pathTable, topK, pregelGraph)
    }
    pathTable.entries.foreach(println)
    pathTable.entries.map(entry => entry.path).toList
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
          Loggers.default.debug(s"stateTable: ${stateTable}, pathTable: ${pathTable.entries}")

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

          /*
          Loggers.default.debug(s"tableA: ${tableA.entries}")
          Loggers.default.debug(s"tableB: ${tableB.entries}")
          Loggers.default.debug(s"filteredTableB: ${filteredTableB.entries}")
           */

          tableA.mergeWithTable(filteredTableB, topK)
        }
      )
    //println(s"initPathTable: ${result.entries}")
    //println(s"initPathTable size: ${result.entries.size}")
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

    /*
        println(s"entries: ${pathsTable.entries}")
        println(s"remainingLength: ${groupedEntries.values.head.head.remainingLength - 1}")
        println("groupedEntries: ")
        groupedEntries.foreach(println)
     */

    val result = groupedEntries.values
      .flatMap(entries => {
        //println(s"Entries in group: $entries")
        val nextEntries = findNextEntries(entries.head.path.startNode,
                                          entries.head.remainingLength - 1,
                                          null,
                                          entries.length,
                                          vertexMap)
        /*
        println(s"entries: ${entries.length}")
        println(s"nextEntries: ${nextEntries.entries.length}")
         */

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
    /*
    println(s"result: $result")
     */

    PathWeightTable(result, topK)
  }

  private def findNextEntries(parentVertex: VertexId,
                              remainingLength: Int,
                              interval: Interval,
                              groupLength: Int,
                              vertexMap: Map[VertexId, PregelVertex]): LengthWeightTable = {
    LengthWeightTable(history = List(),
                      actives = vertexMap(parentVertex).intervalsState.firstTable
                        .getEntriesByLength(remainingLength),
                      topK = groupLength)
  }

  private def pairwiseExtendPaths(pathTable: PathWeightTable, lengthWeightTable: LengthWeightTable): PathWeightTable = {

    //println("Pairwise extend paths")
    //println(s"pathTable: ${pathsTable.entries}")
    //println(s"lengthWeightTable: ${lengthWeightTable.entries}")

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

  def legacyConstructPaths(pregelGraph: Graph[PregelVertex, Properties]): List[TemporalPath] = {
    val destinationTriplets = pregelGraph.triplets
      .filter(triplet => triplet.dstAttr.constState.destination)

    /*
    destinationTriplets
      .collect()
      .foreach(triplet => println(s"Source: ${triplet.srcId}, Destination: ${triplet.dstId}"))
     */

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
    //println(s"Parent: $parentVertex, Length: $remainingLength, Interval: $interval")
    //println(s"Table: ${vertexMap(parentVertex).intervalsState.firstTable}")

    vertexMap(parentVertex).intervalsState.firstTable
      .getEntriesByLength(remainingLength)
      .minBy(_.weight)
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

}
