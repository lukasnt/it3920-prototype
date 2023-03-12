package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{Interval, PathQuery, Properties, TemporalGraph}
import com.lukasnt.spark.models.{ArbitraryQuery, ConstQuery, SequencedQueries, VariableQuery}
import org.apache.spark.graphx.EdgeTriplet

object SubgraphFilterExecutor {

  def executeSubgraphFilter(sequencedQueries: SequencedQueries, temporalGraph: TemporalGraph): TemporalGraph = {
    val nodeTests        = sequencedQueries.sequence.map(query => extractConstQuery(query._1).nodeTest)
    val aggTests         = sequencedQueries.sequence.map(query => query._2.aggTest)
    val aggIntervalTests = sequencedQueries.sequence.map(query => query._2.aggIntervalTest)

    val filteredGraph = temporalGraph.subgraph(
      epred = triplet => tripletSatisfiesTests(triplet, aggTests, aggIntervalTests, nodeTests),
      vpred = (_, atr) => nodeSatisfiesTests(atr, nodeTests))

    filteredGraph
  }

  private def tripletSatisfiesTests(triplet: EdgeTriplet[Properties, Properties],
                                    aggTests: List[(Properties, Properties, Properties) => Boolean],
                                    aggIntervalTests: List[(Interval, Interval) => Boolean],
                                    nodeTests: List[Properties => Boolean]): Boolean = {
    val sequenceLength            = Math.min(aggTests.length, Math.min(aggIntervalTests.length, nodeTests.length))
    var satisfiesNodeTests        = false
    var satisfiesAggTests         = false
    var satisfiesAggIntervalTests = false
    for (i <- 0 until sequenceLength - 1) {
      satisfiesNodeTests = satisfiesNodeTests || (nodeTests(i)(triplet.srcAttr) && nodeTests(i + 1)(triplet.dstAttr))
      satisfiesAggTests = satisfiesAggTests || aggTests(i)(triplet.srcAttr, triplet.dstAttr, triplet.attr)
      satisfiesAggIntervalTests = satisfiesAggIntervalTests || aggIntervalTests(i)(triplet.srcAttr.interval,
                                                                                   triplet.attr.interval)
    }
    satisfiesAggTests && satisfiesAggIntervalTests && satisfiesNodeTests
  }

  private def nodeSatisfiesTests(node: Properties, nodeTests: List[Properties => Boolean]): Boolean = {
    nodeTests.map(func => func(node)).reduceLeft((a, b) => a || b)
  }

  private def extractConstQuery(genericQuery: PathQuery): ConstQuery = {
    genericQuery match {
      case q: ConstQuery     => q
      case q: VariableQuery  => q.constQuery
      case q: ArbitraryQuery => q.constQuery
      case _                 => new ConstQuery()
    }
  }
  /*
  private def graphFromTriplets[VD: ClassTag, ED: ClassTag](triplets: RDD[EdgeTriplet[VD, ED]]): Graph[VD, ED] = {
    val vertices = triplets
      .flatMap(triplet => List((triplet.srcId, triplet.srcAttr), (triplet.dstId, triplet.dstAttr)))
      .distinct() // This might also not be working because of triplets uniqueness
    val edges = triplets.map(triplet => Edge(triplet.srcId, triplet.dstId, triplet.attr))
    Graph.apply(vertices, edges)
  }


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

      val result: TemporalGraph = graphFromTriplets[Properties, Properties](accumulatedTriplets)
      result
    }
 */

}
