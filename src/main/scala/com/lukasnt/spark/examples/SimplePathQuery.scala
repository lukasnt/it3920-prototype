package com.lukasnt.spark.examples

import com.lukasnt.spark.models.Types.TemporalPregelGraph
import com.lukasnt.spark.models.{ConstPathQuery, PathAggFunc, SequencedPathQueries}
import com.lukasnt.spark.operators.PathQueryPregelRunner

import java.time.ZonedDateTime

object SimplePathQuery {

  def result(): TemporalPregelGraph[ZonedDateTime] = {
    val temporalGraph = SimpleSNBLoader.load()

    PathQueryPregelRunner.run(temporalGraph)
  }

  def exampleQuery(): SequencedPathQueries = {
    val query = new SequencedPathQueries(
      List(
        (new ConstPathQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
         new PathAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstPathQuery(_ => 1, tp => tp.typeLabel == "Person"), new PathAggFunc())
      ))
    query
  }

  def triplePathQuery(): SequencedPathQueries = {
    val query = new SequencedPathQueries(
      List(
        (new ConstPathQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
         new PathAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstPathQuery(_ => 1, tp => tp.typeLabel == "Person"),
         new PathAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstPathQuery(_ => 1, tp => tp.typeLabel == "Person"), new PathAggFunc())
      ))
    query
  }

  def emptyQuery(): SequencedPathQueries = {
    val query = new SequencedPathQueries(
      List(
        (new ConstPathQuery(), new PathAggFunc()),
        (new ConstPathQuery(), new PathAggFunc())
      ))
    query
  }

}
