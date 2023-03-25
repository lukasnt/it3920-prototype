package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph

abstract class WeightMapExecutor extends Serializable {

  def weightMap(temporalGraph: TemporalGraph): TemporalGraph

}
