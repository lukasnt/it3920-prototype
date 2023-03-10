package com.lukasnt.spark.examples

import com.lukasnt.spark.executors.QueryPregelRunner
import com.lukasnt.spark.models.Types.TemporalPregelGraph
import com.lukasnt.spark.models.{ConstQuery, QueryAggFunc, SequencedQueries}

object SimplePathQuery {

  def result(): TemporalPregelGraph = {
    val temporalGraph = SimpleSNBLoader.load()

    QueryPregelRunner.run(temporalGraph)
  }

  def exampleQuery(): SequencedQueries = {
    val query = new SequencedQueries(
      List(
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person"), new QueryAggFunc())
      ))
    query
  }

  def triplePathQuery(): SequencedQueries = {
    val query = new SequencedQueries(
      List(
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Hans"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person"),
         new QueryAggFunc())
      ))
    query
  }

  def emptyQuery(): SequencedQueries = {
    val query = new SequencedQueries(
      List(
        (new ConstQuery(), new QueryAggFunc()),
        (new ConstQuery(), new QueryAggFunc())
      ))
    query
  }

}
