package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalPath
import com.lukasnt.spark.models.Types.TemporalGraph

class QueryResult(val queriedGraph: TemporalGraph, val paths: List[TemporalPath] = List()) {

  def asGraphList: List[TemporalGraph] = {
    paths.map(path => path.asTemporalGraph(queriedGraph))
  }
  
}
