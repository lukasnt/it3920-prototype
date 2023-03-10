package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{PathQuery, Properties, TemporalGraph}
import com.lukasnt.spark.models.{ArbitraryQuery, ConstQuery, SequencedQueries, VariableQuery}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SubgraphFilterExecutor {

  def executeSubgraphFilter(sequencedQueries: SequencedQueries, temporalGraph: TemporalGraph): TemporalGraph = {

    val accumulatedTriplets = sequencedQueries.sequence.zipWithIndex
      .map(indexedQuery => {
        val ((genericQuery, aggFunc), seqNum) = indexedQuery

        val constQuery      = extractConstQuery(genericQuery)
        val testFunc        = constQuery.testFunc
        val aggTest         = aggFunc.aggTest
        val aggIntervalTest = aggFunc.aggIntervalTest

        val triplets = temporalGraph.triplets.filter(t => {
          aggTest(t.srcAttr, t.dstAttr, t.attr) &&
          aggIntervalTest(t.srcAttr.interval, t.dstAttr.interval, t.attr.interval) &&
          testFunc(t.srcAttr)
        })
        triplets
      })
      .reduceLeft((accTriplets, triplets) => {
        accTriplets.union(triplets) // This is not working as expected, because of triplets uniqueness
      })

    val result: TemporalGraph = graphFromTriplets[Properties, Properties](accumulatedTriplets)
    result
  }

  private def extractConstQuery(genericQuery: PathQuery): ConstQuery = {
    genericQuery match {
      case q: ConstQuery     => q
      case q: VariableQuery  => q.constQuery
      case q: ArbitraryQuery => q.constQuery
      case _                 => new ConstQuery()
    }
  }

  private def graphFromTriplets[VD: ClassTag, ED: ClassTag](triplets: RDD[EdgeTriplet[VD, ED]]): Graph[VD, ED] = {
    val vertices = triplets
      .flatMap(triplet => List((triplet.srcId, triplet.srcAttr), (triplet.dstId, triplet.dstAttr)))
      .distinct() // This might also not be working because of triplets uniqueness
    val edges = triplets.map(triplet => Edge(triplet.srcId, triplet.dstId, triplet.attr))
    Graph.apply(vertices, edges)
  }

}
