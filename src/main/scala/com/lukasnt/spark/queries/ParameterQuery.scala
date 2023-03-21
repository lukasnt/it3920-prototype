package com.lukasnt.spark.queries

import com.lukasnt.spark.models.Types.{Interval, PropertyEdge, PropertyVertex}

class ParameterQuery() {

  private var sourcePredicate: PropertyVertex => Boolean = { _ =>
    true
  }

  private var intermediatePredicate: PropertyEdge => Boolean = { _ =>
    true
  }

  private var destinationPredicate: PropertyVertex => Boolean = { _ =>
    true
  }

  private var intervalRelation: (Interval, Interval) => Interval = { (a, b) =>
    b
  }

  private var weightMap: PropertyEdge => Float = { _ =>
    1.0f
  }

  private var minLength: Int = 1

  private var maxLength: Int = 10

  private var topK: Int = 5

}

object ParameterQuery {

  def builder() = new ParameterQueryBuilder()

  class ParameterQueryBuilder {

    private val query = new ParameterQuery()

    def build(): ParameterQuery = {
      query
    }

    def fromQuery(query: ParameterQuery): ParameterQueryBuilder = {
      this.query.sourcePredicate = query.sourcePredicate
      this.query.intermediatePredicate = query.intermediatePredicate
      this.query.destinationPredicate = query.destinationPredicate
      this.query.intervalRelation = query.intervalRelation
      this.query.weightMap = query.weightMap
      this.query.minLength = query.minLength
      this.query.maxLength = query.maxLength
      this.query.topK = query.topK
      this
    }

    def withSourcePredicate(predicate: PropertyVertex => Boolean): ParameterQueryBuilder = {
      query.sourcePredicate = predicate
      this
    }

    def withIntermediatePredicate(predicate: PropertyEdge => Boolean): ParameterQueryBuilder = {
      query.intermediatePredicate = predicate
      this
    }

    def withDestinationPredicate(predicate: PropertyVertex => Boolean): ParameterQueryBuilder = {
      query.destinationPredicate = predicate
      this
    }

    def withIntervalRelation(relation: (Interval, Interval) => Interval): ParameterQueryBuilder = {
      query.intervalRelation = relation
      this
    }

    def withWeightMap(weightMap: PropertyEdge => Float): ParameterQueryBuilder = {
      query.weightMap = weightMap
      this
    }

    def withMinLength(minLength: Int): ParameterQueryBuilder = {
      query.minLength = minLength
      this
    }

    def withMaxLength(maxLength: Int): ParameterQueryBuilder = {
      query.maxLength = maxLength
      this
    }

    def withTopK(topK: Int): ParameterQueryBuilder = {
      query.topK = topK
      this
    }

  }

}
