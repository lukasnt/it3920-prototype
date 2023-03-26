package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.{Properties, TemporalGraph}
import com.lukasnt.spark.queries.{ConstState, ConstStateMessages, SequencedQueries}
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph}

class ConstPregel(sequencedQueries: SequencedQueries)
    extends PregelExecutor[(Properties, List[ConstState]), Properties, ConstStateMessages] {

  // Create the init states beforehand as TemporaryPathQuery is not serializable
  val initStates: List[ConstState] = sequencedQueries.createInitStates()

  // Extract the test functions from the queries
  val nodeTests: List[Properties => Boolean] = sequencedQueries.sequence.map(s => s._1.nodeTest)

  override def maxIterations(): Int = 100

  override def activeDirection(): EdgeDirection = EdgeDirection.Either

  override def initMessages(): ConstStateMessages = new ConstStateMessages(initStates)

  override def preprocessGraph(temporalGraph: TemporalGraph): Graph[(Properties, List[ConstState]), Properties] = {
    temporalGraph.mapVertices((id, attr) => (attr, initStates))
  }

  override def vertexProgram(vertexId: Long,
                             currentState: (Properties, List[ConstState]),
                             mergedMessage: ConstStateMessages): (Properties, List[ConstState]) = {
    val (node, stateSequence) = currentState
    val newStateSequence =
      stateSequence.map(
        state =>
          ConstState
            .builder()
            .fromState(state)
            .applySourceTest(node, nodeTests(state.seqNum))
            .build())
    (node, newStateSequence)
  }

  override def sendMessage(
      edge: EdgeTriplet[(Properties, List[ConstState]), Properties]): Iterator[(Long, ConstStateMessages)] = {
    Iterator.empty
  }

  override def mergeMessage(msgA: ConstStateMessages, msgB: ConstStateMessages): ConstStateMessages = {
    msgA.merge(msgB)
  }
}

object ConstPregel {

  def apply(temporalGraph: TemporalGraph,
            sequencedQueries: SequencedQueries): Graph[(Properties, List[ConstState]), Properties] = {
    new ConstPregel(sequencedQueries).run(temporalGraph)
  }

}
