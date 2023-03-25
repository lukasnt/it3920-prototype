package com.lukasnt.spark.examples

import com.lukasnt.spark.executors.ConstPregel
import com.lukasnt.spark.models.Types.SequencedPregelGraph
import com.lukasnt.spark.queries.{ConstQuery, QueryAggFunc, SequencedQueries}

object SimpleConstQueries {

  def result(): SequencedPregelGraph = {
    val temporalGraph = SimpleSNBLoader.load()

    ConstPregel(temporalGraph, exampleQuery())
  }

  def exampleQuery(): SequencedQueries = {
    new SequencedQueries(List(
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
        new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"), new QueryAggFunc())
    ))
  }

  def triplePathQuery(): SequencedQueries = {
    new SequencedQueries(List(
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
        new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Bryn"),
        new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
        new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
    ))

  }

  def quadPathQuery(): SequencedQueries = {
    new SequencedQueries(List(
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

  def knowsRelationsQuery(): SequencedQueries = {
    new SequencedQueries(List(
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
        new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
        new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
    ))

  }

  def emptyQuery(): SequencedQueries = {
    new SequencedQueries(List(
      (new ConstQuery(), new QueryAggFunc()),
      (new ConstQuery(), new QueryAggFunc())
    ))
  }

}
