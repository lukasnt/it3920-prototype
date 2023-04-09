package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.{Interval, PregelVertex, Properties}
import com.lukasnt.spark.queries.ParameterQuery
import com.lukasnt.spark.util
import com.lukasnt.spark.util.{IntervalStates, LengthWeightTable, PathWeightTable}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD

class ParameterPathsConstruction(parameterQuery: ParameterQuery)
    extends PathsConstructionExecutor[PregelVertex, Properties] {

  private val minLength: Int                                 = parameterQuery.minLength
  private val maxLength: Int                                 = parameterQuery.maxLength
  private val topK: Int                                      = parameterQuery.topK
  private val nextInterval: (Interval, Interval) => Interval = parameterQuery.temporalPathType.nextInterval

  override def constructPaths(pregelGraph: Graph[PregelVertex, Properties]): PathWeightTable = {
    var pathTable = createInitPathTable(findDestinationTriplets(pregelGraph), topK, minLength, maxLength)
    while (pathTable.entries.exists(entry => entry.remainingLength > 1)) {
      pathTable = extendPaths(pathTable, topK, pregelGraph)
    }
    pathTable
  }

  private def findDestinationTriplets(
      pregelGraph: Graph[PregelVertex, Properties]): RDD[EdgeTriplet[PregelVertex, Properties]] = {
    pregelGraph.triplets
      .filter(triplet => triplet.dstAttr.queryState.destination)
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
            tableA.mergeWithTable(util.PathWeightTable(distinctEntries, topK), topK)
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
    // Retrieve the states of vertices that are start-nodes of the current paths, in the form of triplets
    val edgeSet = pathsTable.entries.map(entry => entry.path.startEdge).toSet
    val tripletsMap = pregelGraph.triplets
      .filter(triplet => edgeSet.exists(e => e.srcId == triplet.srcId && e.dstId == triplet.dstId))
      .map(triplet => ((triplet.srcId, triplet.dstId), triplet))
      .collect()
      .toMap

    // Group the paths by their path-vertex-sequence, interval and remaining length
    val groupedEntries =
      pathsTable.entries.groupBy(entry => (entry.path.vertexSequence, entry.interval, entry.remainingLength))

    println(s"Number of groups: ${groupedEntries.size}, number of entries total: ${groupedEntries.map(_._2.size).sum}")

    val result = groupedEntries
      .map {
        case ((pathVertexSequence, interval, remainingLength), entries) =>
          if (remainingLength - 1 > 0) {
            pairwiseExtendPaths(
              pathTable = util.PathWeightTable(entries, entries.length),
              intervalEntries = findNextEntries(remainingLength - 1,
                                                interval,
                                                entries.length,
                                                tripletsMap((pathVertexSequence.head, pathVertexSequence(1))))
            )
          } else util.PathWeightTable(entries, entries.length)
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

  private def findNextEntries(pathLength: Int,
                              interval: Interval,
                              groupCount: Int,
                              triplet: EdgeTriplet[PregelVertex, Properties]): List[IntervalStates.IntervalEntry] = {
    triplet.srcAttr.intervalStates
      .intervalFilteredStates((a, _) => { nextInterval(a, triplet.attr.interval) == interval }, triplet.attr.interval)
      .lengthFilteredStates(pathLength)
      .flattenEntries(groupCount)
  }

}

object ParameterPathsConstruction {

  def apply(pregelGraph: Graph[PregelVertex, Properties], parameterQuery: ParameterQuery): PathWeightTable = {
    new ParameterPathsConstruction(parameterQuery).constructPaths(pregelGraph)
  }

}
