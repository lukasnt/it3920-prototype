package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.{AttrEdge, Interval, PregelVertex, Properties}
import com.lukasnt.spark.queries.{IntervalStates, LengthWeightTable, ParameterQuery, PathWeightTable}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

class ParameterPathsConstruction(parameterQuery: ParameterQuery)
    extends PathsConstructionExecutor[PregelVertex, Properties] {

  val minLength: Int                                     = parameterQuery.minLength
  val maxLength: Int                                     = parameterQuery.maxLength
  val topK: Int                                          = parameterQuery.topK
  val weightMap: AttrEdge => Float                       = parameterQuery.weightMap
  val validEdgeInterval: (Interval, Interval) => Boolean = parameterQuery.temporalPathType.validEdgeInterval
  val nextInterval: (Interval, Interval) => Interval     = parameterQuery.temporalPathType.nextInterval

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
    val pathTable = triplets
      .map(triplet => (triplet.dstId, triplet.dstAttr.intervalStates))
      .aggregate[PathWeightTable](PathWeightTable(List(), topK))(
        seqOp = {
          case (pathTable, (vertex, intervalStates)) =>
            // Skip the table if the destination vertex is already in the path table
            if (pathTable.destinationVertexExists(vertex))
              pathTable
            else
              mergeIntervalStatesToPathTable(pathTable, intervalStates, vertex, topK, minLength, maxLength)
        },
        combOp = {
          case (tableA, tableB) =>
            // Filter out duplicate entries (same destination node)
            val distinctEntries =
              tableB.entries.filter(entry => !tableA.destinationVertexExists(entry.destinationVertex))
            tableA.mergeWithTable(PathWeightTable(distinctEntries, topK), topK)
        }
      )

    //println(pathTable)

    pathTable
  }

  private def mergeIntervalStatesToPathTable(pathTable: PathWeightTable,
                                             intervalStates: IntervalStates,
                                             vertex: VertexId,
                                             topK: Int,
                                             minLength: Int,
                                             maxLength: Int): PathWeightTable = {
    // Merge the aggregated path-table into all interval-tables merged, resulting in one path-table
    pathTable.mergeWithTable(
      // Merge all interval tables into one path-table
      other = intervalStates.intervalTables
        .map(intervalTable => {
          createPathTable(intervalTable.table, vertex, topK, minLength, maxLength, intervalTable.interval)
        })
        .reduce((tableA, tableB) => tableA.mergeWithTable(tableB, topK)),
      topK = topK
    )
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
            Edge[Properties](srcId = entry.parentId, dstId = vertex, attr = new Properties(interval, "", Map())))
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
        val startEdge = entry.path.startEdge
        (entry.path.vertexSequence, entry.interval, entry.remainingLength)
      })

    println("Grouped entries: ")
    println(s"Number of groups: ${groupedEntries.size}, number of entries total: ${groupedEntries.map(_._2.size).sum}")
    //groupedEntries.foreach(println)

    val result = groupedEntries
      .map {
        case ((pathVertexSequence, interval, remainingLength), entries) =>
          if (remainingLength - 1 > 0) {
            println(s"Group: ($pathVertexSequence, $interval, $remainingLength), size: ${entries.size}")
            /*
            println(s"Group: ($srcId, $dstId, $interval, $remainingLength), size: ${entries.size}")
            if (entries.size > vertexMap(srcId).intervalStates.intervalTables.size)
              println(s"entries > intervalsState: $entries")

             */

            // Find the next entries for this group and pairwise extend the paths
            pairwiseExtendPaths(
              pathTable = PathWeightTable(entries, entries.length),
              intervalEntries =
                findNextEntries(pathVertexSequence.head, remainingLength - 1, interval, entries.length, vertexMap)
            )
          } else PathWeightTable(entries, entries.length)
      }
      .reduce((tableA, tableB) => tableA.mergeWithTable(tableB, topK))

    println(s"Number of entries after extendPaths: ${result.entries.size}")

    result
  }

  private def pairwiseExtendPaths(pathTable: PathWeightTable,
                                  intervalEntries: List[IntervalStates.IntervalEntry]): PathWeightTable = {
    PathWeightTable(
      tableEntries = pathTable.entries.sortBy(_.weight).zip(intervalEntries.sortBy(_.entry.weight)).map {
        case (pathEntry, IntervalStates.IntervalEntry(interval, entry)) =>
          PathWeightTable.Entry(
            interval = interval,
            remainingLength = entry.length,
            weight = pathEntry.weight,
            path = TemporalPath(
              Edge[Properties](entry.parentId, pathEntry.path.startNode, new Properties(interval, "", Map()))
            ) + pathEntry.path
          )
      },
      topK = pathTable.entries.length
    )
  }

  private def findNextEntries(parentVertex: VertexId,
                              pathLength: Int,
                              interval: Interval,
                              groupCount: Int,
                              vertexMap: Map[VertexId, PregelVertex]): List[IntervalStates.IntervalEntry] = {
    println(s"findNextEntries($parentVertex, $pathLength, $interval, $groupCount, vertexMap)")
    //println(s"vertexMap($parentVertex) = ${vertexMap(parentVertex)}")

    val result: List[IntervalStates.IntervalEntry] = vertexMap(parentVertex).intervalStates
      .intervalFilteredStates(validEdgeInterval, interval)
      .lengthFilteredStates(pathLength)
      .flattenEntries(groupCount)

    println(s"findNextEntries result: $result")
    if (result.length < groupCount)
      println(s"intervalStates: ${vertexMap(parentVertex).intervalStates}")

    result
  }

  /*
  private def findNextTable(parentVertex: VertexId,
                            remainingLength: Int,
                            interval: Interval,
                            groupCount: Int,
                            vertexMap: Map[VertexId, PregelVertex]): LengthWeightTable = {
    vertexMap(parentVertex).intervalStates
      .intervalFilteredTable(validEdgeInterval, interval, topK = -1)
      .filterByLength(remainingLength, groupCount)
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
                               new Properties(interval, "", Map()))
            ) + pathEntry.path
          )
      },
      topK = pathTable.entries.length
    )
  }
   */
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
