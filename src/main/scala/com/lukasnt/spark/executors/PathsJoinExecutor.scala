package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalPregelGraph
import com.lukasnt.spark.models.{SequencedQueries, TemporalPath}
import org.apache.spark.rdd.RDD

object PathsJoinExecutor {

  def joinSequence(sequencedPathQueries: SequencedQueries,
                   pathsSequence: List[RDD[TemporalPath]]): RDD[TemporalPath] = {
    pathsSequence.reduceLeft((accumulatedPaths, constPaths) => {
      val joinedPaths = accumulatedPaths
        .groupBy(path => path.getEndNode)
        .join(constPaths.groupBy(path => path.getStartNode))
        .flatMap(pathsPairs => {
          // TODO: Add the aggregation functions (for both test and interval-relation) from queries

          // TODO: Check if this is correct
          val (nodeId, (accPaths, cPaths)) = pathsPairs
          val cPathsEdges                  = cPaths.map(c => c.edgeSequence.head).toList
          accPaths.flatMap(p => p.outerJoinWithPaths(cPaths.toList))
        })

      joinedPaths
    })
  }

  def createConstPaths(sequencedPathQueries: SequencedQueries,
                       temporalPregelGraph: TemporalPregelGraph): List[RDD[TemporalPath]] = {
    sequencedPathQueries.sequence.zipWithIndex
      .map(seqPathQuery => {
        val ((query, aggFunc), seqNum) = seqPathQuery

        val seqLen  = sequencedPathQueries.sequence.length
        val aggTest = aggFunc.aggTest

        val temporalPath =
          if (seqNum < seqLen - 1)
            temporalPregelGraph
              .subgraph(e =>
                e.srcAttr._2(seqNum).testSuccess && e.dstAttr._2.last.testSuccess && aggTest(null, null, e.attr))
              .edges
              .map(edge => new TemporalPath(List(edge)))
          else null
        temporalPath
      })
      .take(sequencedPathQueries.sequence.length - 1)
  }

}
