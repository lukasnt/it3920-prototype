package com.lukasnt.spark.examples

import com.lukasnt.spark.executors.ConstPregel
import com.lukasnt.spark.models.Types.Properties
import com.lukasnt.spark.queries.ConstQuery.AggFunc
import com.lukasnt.spark.queries.{ConstQueries, ConstQuery, ConstState}
import org.apache.spark.graphx.Graph

object SimpleConstQueries {

  def result(): Graph[(Properties, List[ConstState]), Properties] = {
    val temporalGraph = SimpleSNBLoader.load()

    ConstPregel(temporalGraph, exampleQuery())
  }

  def exampleQuery(): ConstQueries = {
    new ConstQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
         new AggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"), new AggFunc())
      ))
  }

  def triplePathQuery(): ConstQueries = {
    new ConstQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
         new AggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Bryn"),
         new AggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new AggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
      ))

  }

  def quadPathQuery(): ConstQueries = {
    new ConstQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Almira"),
         new AggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Bryn"),
         new AggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Hans"),
         new AggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new AggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
      ))

  }

  def knowsRelationsQuery(): ConstQueries = {
    new ConstQueries(
      List(
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new AggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person")),
        (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person"),
         new AggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
      ))

  }

  def emptyQuery(): ConstQueries = {
    new ConstQueries(
      List(
        (new ConstQuery(), new AggFunc()),
        (new ConstQuery(), new AggFunc())
      ))
  }

}
