package com.lukasnt.spark.examples

import com.lukasnt.spark.models.{ConstQuery, QueryAggFunc, WeightedQueries}

object SimpleWeightedQueries {

  def exampleQuery: WeightedQueries = {
    new WeightedQueries(
      List(
        (new ConstQuery(_ => 1.0f, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Almira"),
          new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person",
            aggCost = (pathCost, edge) => pathCost + edge.typeLabel.length)),
        (new ConstQuery(_ => 2.0f, tp => tp.typeLabel == "Person" && tp.properties("firstName") == "Bryn"),
          new QueryAggFunc(aggTest = (_, _, e) => e.typeLabel == "Person_knows_Person",
            aggCost = (pathCost, edge) => pathCost + edge.typeLabel.length)),
        (new ConstQuery(_ => 3.0f, tp => tp.typeLabel == "Person"),
          new QueryAggFunc(aggTest = (_, _, _) => false, aggIntervalTest = (_, _) => false))
      )
    )
  }
}
