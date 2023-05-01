package com.lukasnt.spark.experiments

case class ExperimentExecutionInfo(
    // In ms
    val subgraphPhaseTime: Long,
    val weightMapPhaseTime: Long,
    val pregelPhaseTime: Long,
    val pathConstructionPhaseTime: Long,
    val totalExecutionTime: Long
) {
  override def toString: String = {
    s"Subgraph Phase Time: $subgraphPhaseTime ms\n" +
      s"Weight Map Phase Time: $weightMapPhaseTime ms\n" +
      s"Pregel Phase Time: $pregelPhaseTime ms\n" +
      s"Path Construction Phase Time: $pathConstructionPhaseTime ms\n" +
      s"Total Execution Time: $totalExecutionTime ms"
  }
}
