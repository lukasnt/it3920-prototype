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
    new UnweightedQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"), new QueryAggFunc())
      ))
  }

  def triplePathQuery(): UnweightedQueries = {
    new UnweightedQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Bryn"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
      ))

  }

  def quadPathQuery(): UnweightedQueries = {
    new UnweightedQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Bryn"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Hans"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
      ))

  }

  def knowsRelationsQuery(): UnweightedQueries = {
    new UnweightedQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
      ))

  }

  def emptyQuery(): UnweightedQueries = {
    new UnweightedQueries(
      List(
        (new ConstQuery(), new QueryAggFunc()),
        (new ConstQuery(), new QueryAggFunc())
      ))
  }

}
