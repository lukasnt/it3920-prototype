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

}
