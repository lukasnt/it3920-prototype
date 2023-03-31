package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalPathType
import com.lukasnt.spark.models.Types.{AttrEdge, AttrVertex}

class ParameterQuery() {

  private var _temporalPathType: TemporalPathType          = TemporalPathType.Continuous
  private var _sourcePredicate: AttrVertex => Boolean      = (v) => { v.attr.interval != null }
  private var _intermediatePredicate: AttrEdge => Boolean  = (e) => { e.attr.interval != null }
  private var _destinationPredicate: AttrVertex => Boolean = (v) => { v.attr.interval != null }
  private var _weightMap: AttrEdge => Float                = (_) => { 1.0f }
  private var _minLength: Int                              = 1
  private var _maxLength: Int                              = 10
  private var _topK: Int                                   = 5

  def sourcePredicate: AttrVertex => Boolean      = this._sourcePredicate
  def intermediatePredicate: AttrEdge => Boolean  = this._intermediatePredicate
  def destinationPredicate: AttrVertex => Boolean = this._destinationPredicate
  def temporalPathType: TemporalPathType          = this._temporalPathType
  def weightMap: AttrEdge => Float                = this._weightMap
  def minLength: Int                              = this._minLength
  def maxLength: Int                              = this._maxLength
  def topK: Int                                   = this._topK
}

object ParameterQuery {

  def builder() = new ParameterQueryBuilder()

  class ParameterQueryBuilder {

    private val query = new ParameterQuery()

    def build(): ParameterQuery = {
      query
    }

    def fromQuery(query: ParameterQuery): ParameterQueryBuilder = {
      this.query._sourcePredicate = query._sourcePredicate
      this.query._intermediatePredicate = query._intermediatePredicate
      this.query._destinationPredicate = query._destinationPredicate
      this.query._temporalPathType = query._temporalPathType
      this.query._weightMap = query._weightMap
      this.query._minLength = query._minLength
      this.query._maxLength = query._maxLength
      this.query._topK = query._topK
      this
    }

    def withSourcePredicate(predicate: AttrVertex => Boolean): ParameterQueryBuilder = {
      query._sourcePredicate = predicate
      this
    }

    def withIntermediatePredicate(predicate: AttrEdge => Boolean): ParameterQueryBuilder = {
      query._intermediatePredicate = predicate
      this
    }

    def withDestinationPredicate(predicate: AttrVertex => Boolean): ParameterQueryBuilder = {
      query._destinationPredicate = predicate
      this
    }

    def withPathType(pathType: TemporalPathType): ParameterQueryBuilder = {
      query._temporalPathType = pathType
      this
    }

    def withWeightMap(weightMap: AttrEdge => Float): ParameterQueryBuilder = {
      query._weightMap = weightMap
      this
    }

    def withMinLength(minLength: Int): ParameterQueryBuilder = {
      query._minLength = minLength
      this
    }

    def withMaxLength(maxLength: Int): ParameterQueryBuilder = {
      query._maxLength = maxLength
      this
    }

    def withTopK(topK: Int): ParameterQueryBuilder = {
      query._topK = topK
      this
    }

  }
}
