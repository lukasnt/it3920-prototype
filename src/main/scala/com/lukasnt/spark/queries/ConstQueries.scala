package com.lukasnt.spark.queries

import com.lukasnt.spark.queries.ConstQuery.AggFunc
import com.lukasnt.spark.util.ConstState

class ConstQueries(val sequence: List[(ConstQuery, AggFunc)] = List()) {

  def createInitStates(): List[ConstState] = {
    sequence.zipWithIndex.map(q => new ConstState(q._2))
  }

}
