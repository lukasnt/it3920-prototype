package com.lukasnt.spark.examples

import com.lukasnt.spark.models.TemporalPathType
import com.lukasnt.spark.queries.ParameterQuery

object SimpleParameterQueries {

  def testQuery(): ParameterQuery = {
    ParameterQuery
      .builder()
      .withPathType(TemporalPathType.Continuous)
      .withSourcePredicate(_ => true)
      .withIntermediatePredicate(_ => true)
      .withDestinationPredicate(_ => true)
      .withWeightMap(_ => 1.0f)
      .withMinLength(1)
      .withMaxLength(10)
      .withTopK(5)
      .build()
  }

  def interactionPaths(city1: String = "1226",
                       city2: String = "1363",
                       minLength: Int = 4,
                       topK: Int = 10,
                       pathType: TemporalPathType = TemporalPathType.Continuous): ParameterQuery = {
    ParameterQuery
      .builder()
      .withPathType(pathType)
      .withSourcePredicate(s => s.attr.typeLabel == "Person" && s.attr.properties("gender") == "male")
      .withIntermediatePredicate(e => e.attr.typeLabel == "Person_knows_Person")
      .withDestinationPredicate(d => d.attr.typeLabel == "Person" && d.attr.properties("gender") == "female")
      .withWeightMap(e => e.attr.interval.getDuration.toFloat)
      .withMinLength(minLength)
      .withMaxLength(100)
      .withTopK(topK)
      .build()
  }

}
