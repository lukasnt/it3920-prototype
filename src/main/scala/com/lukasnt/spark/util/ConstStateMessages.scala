package com.lukasnt.spark.util

import com.lukasnt.spark.queries.ConstState

class ConstStateMessages(val queryStates: List[ConstState]) extends Serializable {

  def merge(other: ConstStateMessages): ConstStateMessages = {
    new ConstStateMessages(queryStates ++ other.queryStates)
  }

}
