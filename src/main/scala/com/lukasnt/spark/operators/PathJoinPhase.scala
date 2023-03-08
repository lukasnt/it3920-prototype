package com.lukasnt.spark.operators

import com.lukasnt.spark.models.Types.TemporalPregelGraph
import com.lukasnt.spark.models.{SequencedPathQueries, TemporalPath}
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime

object PathJoinPhase {

  def createConstPaths(sequencedPathQueries: SequencedPathQueries,
                       temporalPregelGraph: TemporalPregelGraph[ZonedDateTime]): RDD[TemporalPath] = {
    sequencedPathQueries.sequence.zipWithIndex
      .map(seqPathQuery => {
        val ((query, aggFunc), seqNum) = seqPathQuery
        val temporalPath =
          if (seqNum < sequencedPathQueries.sequence.length - 1)
            temporalPregelGraph
              .subgraph(e => e.srcAttr._2(seqNum).testSuccess && e.dstAttr._2(seqNum + 1).testSuccess)
              .edges
              .map(edge => new TemporalPath(List(edge)))
          else
            temporalPregelGraph
              .subgraph(e => e.srcAttr._2(seqNum).testSuccess)
              .edges
              .map(edge => new TemporalPath(List(edge)))
        temporalPath
      })
      .reduce((a, b) => a.union(b))
  }

}
