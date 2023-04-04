package com.lukasnt.spark.queries

import com.lukasnt.spark.queries.ConstQuery.AggFunc

class ConstQueries(val sequence: List[(ConstQuery, AggFunc)] = List()) {

  def createInitStates(): List[ConstState] = {
    sequence.zipWithIndex.map(q => new ConstState(q._2))
  }

}
