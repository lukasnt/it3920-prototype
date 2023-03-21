package com.lukasnt.spark.examples

import com.lukasnt.spark.queries.{ConstQuery, QueryAggFunc, SequencedQueries}

object SimpleWeightedQueries {

  def exampleQuery: SequencedQueries = {
    new SequencedQueries(List(
      (new ConstQuery(_ => 1.0f, v => v.typeLabel == "Person" && v.properties("firstName") == "Hans"),
        new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person",
          aggCost = (pCost, e) => pCost + e.typeLabel.length)),
      (new ConstQuery(_ => 2.0f, v => v.typeLabel == "Person"),
        new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person",
          aggCost = (pCost, e) => pCost + e.typeLabel.length)),
      (new ConstQuery(_ => 3.0f, v => v.typeLabel == "Person"),
        new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
    ))
  }
}
