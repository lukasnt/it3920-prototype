package com.lukasnt.spark.examples

import com.lukasnt.spark.queries.ParameterQuery

object SimpleParameterQueries {

  def testQuery(): ParameterQuery = {
    ParameterQuery
      .builder()
      .withSourcePredicate(_ => true)
      .withIntermediatePredicate(_ => true)
      .withDestinationPredicate(_ => true)
      .withIntervalRelation((a, b) => b)
      .withWeightMap(_ => 1.0f)
      .withMinLength(1)
      .withMaxLength(10)
      .withTopK(5)
      .build()
  }

  def interactionPaths(city1: String = "1226",
                       city2: String = "1363",
                       minLength: Int = 2,
                       topK: Int = 3): ParameterQuery = {
    ParameterQuery
      .builder()
      .withSourcePredicate(s => s.attr.typeLabel == "Person" && s.attr.properties("gender") == "male")
      .withIntermediatePredicate(e => e.attr.typeLabel == "Person_knows_Person")
      .withDestinationPredicate(d => d.attr.typeLabel == "Person" && d.attr.properties("gender") == "female")
      .withIntervalRelation((a, b) => a.getUnion(b))
      .withWeightMap(e => e.attr.interval.getDuration.toFloat)
      .withMinLength(minLength)
      .withMaxLength(10)
      .withTopK(topK)
      .build()
  }

}
