package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{Interval, Properties, TemporalGraph}
import com.lukasnt.spark.queries.ConstQueries
import org.apache.spark.graphx.EdgeTriplet

class ConstSubgraph(sequencedQueries: ConstQueries) extends SubgraphExecutor {

  private val nodeTests: List[Properties => Boolean] =
    sequencedQueries.sequence.map(query => query._1.nodeTest)
  private val aggTests: List[(Properties, Properties, Properties) => Boolean] =
    sequencedQueries.sequence.map(query => query._2.aggTest)
  private val aggIntervalTests: List[(Interval, Interval) => Boolean] =
    sequencedQueries.sequence.map(query => query._2.aggIntervalTest)

  def subgraph(temporalGraph: TemporalGraph): TemporalGraph = {
    temporalGraph.subgraph(epred = triplet => tripletSatisfiesTests(triplet, aggTests, aggIntervalTests, nodeTests),
                           vpred = (_, atr) => nodeSatisfiesTests(atr, nodeTests))
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

}

object ConstSubgraph {

  def apply(temporalGraph: TemporalGraph, sequencedQueries: ConstQueries): TemporalGraph =
    new ConstSubgraph(sequencedQueries).subgraph(temporalGraph)

}
