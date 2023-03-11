package com.lukasnt.spark.examples

import com.lukasnt.spark.executors.UnweightedPregelRunner
import com.lukasnt.spark.models.Types.TemporalPregelGraph
import com.lukasnt.spark.models.{ConstQuery, QueryAggFunc, UnweightedQueries}

object SimplePathQuery {

  def result(): TemporalPregelGraph = {
    val temporalGraph = SimpleSNBLoader.load()

    UnweightedPregelRunner.run(exampleQuery(), temporalGraph)
  }

  def exampleQuery(): UnweightedQueries = {
    val query = new UnweightedQueries(
      List(
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person"), new QueryAggFunc())
      ))
    query
  }

  def triplePathQuery(): UnweightedQueries = {
    val query = new UnweightedQueries(
      List(
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Bryn"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person"),
         new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _, _) => false))
      ))
    query
  }

  def quadPathQuery(): UnweightedQueries = {
    val query = new UnweightedQueries(
      List(
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
          new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Bryn"),
          new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Hans"),
          new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1, tp => tp.typeLabel == "Person"),
          new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _, _) => false))
      ))
    query
  }

  def emptyQuery(): UnweightedQueries = {
    val query = new UnweightedQueries(
      List(
        (new ConstQuery(), new QueryAggFunc()),
        (new ConstQuery(), new QueryAggFunc())
      ))
    query
  }

}
