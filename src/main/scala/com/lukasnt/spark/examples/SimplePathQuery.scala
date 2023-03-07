package com.lukasnt.spark.examples

import com.lukasnt.spark.models.Types.TemporalPregelGraph
import com.lukasnt.spark.models.{ConstPathQuery, PathAggFunc, TemporalPathQuery}
import com.lukasnt.spark.operators.PathQueryPregel

import java.time.ZonedDateTime

object SimplePathQuery {

  def result(): TemporalPregelGraph[ZonedDateTime] = {
    val temporalGraph = SimpleSNBLoader.load()

    PathQueryPregel.run(temporalGraph)
  }

  def exampleQuery(): TemporalPathQuery = {
    val query = new TemporalPathQuery(
      List(
        (new ConstPathQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
         new PathAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstPathQuery(_ => 1, tp => tp.typeLabel == "Person"), new PathAggFunc())
      ))
    query
  }

  def emptyQuery(): TemporalPathQuery = {
    val query = new TemporalPathQuery(
      List(
        (new ConstPathQuery(), new PathAggFunc()),
        (new ConstPathQuery(), new PathAggFunc())
      ))
    query
  }

}
