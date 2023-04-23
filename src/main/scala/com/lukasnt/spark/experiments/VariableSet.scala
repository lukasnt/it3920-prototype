package com.lukasnt.spark.experiments

import com.lukasnt.spark.executors.{ParameterQueryExecutor, SparkQueryExecutor}
import com.lukasnt.spark.io.{SNBLoader, TemporalGraphLoader}
import com.lukasnt.spark.models.TemporalPathType
import com.lukasnt.spark.models.Types.{AttrEdge, AttrVertex}
import com.lukasnt.spark.queries.ParameterQuery

import java.time.ZonedDateTime

class VariableSet {

  private var _lengthRangeVariables: List[(Int, Int)]            = List((1, 2), (2, 3), (3, 4), (5, 10))
  private var _topKVariables: List[Int]                          = List(1, 3, 10, 25, 50, 100)
  private var _temporalPathTypeVariables: List[TemporalPathType] = List(TemporalPathType.Continuous)

  private var _sourcePredicateVariables: List[AttrVertex => Boolean] = List(
    v => v.attr.typeLabel == "Person"
  )
  private var _intermediatePredicateVariables: List[AttrEdge => Boolean] = List(
    e => e.attr.typeLabel == "Person_knows_Person"
  )
  private var _destinationPredicateVariables: List[AttrVertex => Boolean] = List(
    v => v.attr.typeLabel == "Person"
  )
  private var _weightMapVariables: List[AttrEdge => Float] = List(
    e => e.attr.interval.getDuration.toFloat
  )

  private var _graphLoaderVariables: List[TemporalGraphLoader[ZonedDateTime]] = List(SNBLoader.localSf0_003)
  private var _executorVariables: List[ParameterQueryExecutor]                = List(SparkQueryExecutor())
  private var _sparkExecutorCountVariables: List[Int]                         = List(4)

  def totalCombinations: Int = {
    _lengthRangeVariables.length *
      _topKVariables.length *
      _temporalPathTypeVariables.length *
      _sourcePredicateVariables.length *
      _intermediatePredicateVariables.length *
      _destinationPredicateVariables.length *
      _weightMapVariables.length *
      _graphLoaderVariables.length *
      _executorVariables.length
  }

  def shuffledQueries: List[VariableSet.QueryExecutionSet] = {
    scala.util.Random.shuffle(ascendingQueries)
  }

  def ascendingQueries: List[VariableSet.QueryExecutionSet] = {
    for {
      sparkExecutorCount    <- _sparkExecutorCountVariables
      lengthRange           <- _lengthRangeVariables
      topK                  <- _topKVariables
      temporalPathType      <- _temporalPathTypeVariables
      sourcePredicate       <- _sourcePredicateVariables
      intermediatePredicate <- _intermediatePredicateVariables
      destinationPredicate  <- _destinationPredicateVariables
      weightMap             <- _weightMapVariables
      graphLoader           <- _graphLoaderVariables
      executor              <- _executorVariables
    } yield {
      VariableSet.QueryExecutionSet(
        query = ParameterQuery
          .builder()
          .withMinLength(lengthRange._1)
          .withMaxLength(lengthRange._2)
          .withTopK(topK)
          .withPathType(temporalPathType)
          .withSourcePredicate(sourcePredicate)
          .withIntermediatePredicate(intermediatePredicate)
          .withDestinationPredicate(destinationPredicate)
          .withWeightMap(weightMap)
          .build(),
        graphLoader = graphLoader,
        executor = executor,
        sparkExecutorCount = sparkExecutorCount
      )
    }
  }

  def descendingQueries: List[VariableSet.QueryExecutionSet] = {
    ascendingQueries.reverse
  }

}

object VariableSet {

  def builder(): Builder = new Builder()

  case class QueryExecutionSet(query: ParameterQuery,
                               graphLoader: TemporalGraphLoader[ZonedDateTime],
                               executor: ParameterQueryExecutor,
                               sparkExecutorCount: Int)

  class Builder {

    private val variableSet = new VariableSet()

    def build(): VariableSet = {
      variableSet
    }

    def fromParameterQuery(parameterQuery: ParameterQuery): Builder = {
      variableSet._lengthRangeVariables = List((parameterQuery.minLength, parameterQuery.maxLength))
      variableSet._topKVariables = List(parameterQuery.topK)
      variableSet._temporalPathTypeVariables = List(parameterQuery.temporalPathType)
      variableSet._sourcePredicateVariables = List(parameterQuery.sourcePredicate)
      variableSet._intermediatePredicateVariables = List(parameterQuery.intermediatePredicate)
      variableSet._destinationPredicateVariables = List(parameterQuery.destinationPredicate)
      variableSet._weightMapVariables = List(parameterQuery.weightMap)
      this
    }

    def withLengthRangeVariables(variables: List[(Int, Int)]): Builder = {
      variableSet._lengthRangeVariables = variables
      this
    }

    def withTopKVariables(variables: List[Int]): Builder = {
      variableSet._topKVariables = variables
      this
    }

    def withTemporalPathTypeVariables(variables: List[TemporalPathType]): Builder = {
      variableSet._temporalPathTypeVariables = variables
      this
    }

    def withSourcePredicateVariables(variables: List[AttrVertex => Boolean]): Builder = {
      variableSet._sourcePredicateVariables = variables
      this
    }

    def withIntermediatePredicateVariables(variables: List[AttrEdge => Boolean]): Builder = {
      variableSet._intermediatePredicateVariables = variables
      this
    }

    def withDestinationPredicateVariables(variables: List[AttrVertex => Boolean]): Builder = {
      variableSet._destinationPredicateVariables = variables
      this
    }

    def withWeightMapVariables(variables: List[AttrEdge => Float]): Builder = {
      variableSet._weightMapVariables = variables
      this
    }

    def withGraphLoaderVariables(variables: List[TemporalGraphLoader[ZonedDateTime]]): Builder = {
      variableSet._graphLoaderVariables = variables
      this
    }

    def withExecutorVariables(variables: List[ParameterQueryExecutor]): Builder = {
      variableSet._executorVariables = variables
      this
    }

    def withSparkExecutorCountVariables(variables: List[Int]): Builder = {
      variableSet._sparkExecutorCountVariables = variables
      this
    }

  }

}
