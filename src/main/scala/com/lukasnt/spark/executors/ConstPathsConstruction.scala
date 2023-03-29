package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.Properties
import com.lukasnt.spark.queries.{ConstState, SequencedQueries}
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

class ConstPathsConstruction(sequencedQueries: SequencedQueries)
    extends PathsConstructionExecutor[(Properties, List[ConstState]), Properties] {

  override def constructPaths(pregelGraph: Graph[(Properties, List[ConstState]), Properties]): List[TemporalPath] = {
    val constPathSequence = ConstPathsConstruction.createConstPaths(sequencedQueries, pregelGraph)
    val pathsResult       = ConstPathsConstruction.joinSequence(sequencedQueries, constPathSequence)
    pathsResult.collect().toList
  }

}

object ConstPathsConstruction {

  def apply(pregelGraph: Graph[(Properties, List[ConstState]), Properties],
            sequencedQueries: SequencedQueries): List[TemporalPath] = {
    new ConstPathsConstruction(sequencedQueries).constructPaths(pregelGraph)
  }

  def joinSequence(sequencedPathQueries: SequencedQueries,
                   pathsSequence: List[RDD[TemporalPath]]): RDD[TemporalPath] = {
    pathsSequence.reduceLeft((accumulatedPaths, constPaths) => {
      val joinedPaths = accumulatedPaths
        .groupBy(path => path.endNode)
        .join(constPaths.groupBy(path => path.startNode))
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

  def createConstPaths(
      sequencedPathQueries: SequencedQueries,
      temporalPregelGraph: Graph[(Properties, List[ConstState]), Properties]): List[RDD[TemporalPath]] = {
    sequencedPathQueries.sequence.zipWithIndex
      .map(seqPathQuery => {
        val ((query, aggFunc), seqNum) = seqPathQuery

        val seqLen  = sequencedPathQueries.sequence.length
        val aggTest = aggFunc.aggTest

        val temporalPath =
          if (seqNum < seqLen - 1)
            temporalPregelGraph
              .subgraph(e =>
                e.srcAttr._2(seqNum).intermediate && e.dstAttr._2.last.intermediate && aggTest(null, null, e.attr))
              .edges
              .map(edge => new TemporalPath(List(edge)))
          else null
        temporalPath
      })
      .take(sequencedPathQueries.sequence.length - 1)
  }

}
