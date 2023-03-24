package com.lukasnt.spark.executors

import com.lukasnt.spark.models.Types.TemporalGraph
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Documentation from GraphX:
  * @tparam VD the vertex data type
  * @tparam ED the edge data type
  * @tparam A the Pregel message type
  */
abstract class PregelRunner[VD: ClassTag, ED: ClassTag, A: ClassTag] extends Serializable {

  /**
    * Documentation from GraphX:
    * the maximum number of iterations to run for
    * @return maximum number of iterations
    */
  def maxIterations(): Int

  /**
    * Documentation from GraphX:
    * the direction of edges incident to a vertex that received a message in the previous round on which to run sendMsg.
    * For example, if this is EdgeDirection.Out, only out-edges of vertices that received a message in the previous round will run.
    * The default is EdgeDirection.Either, which will run sendMsg on edges where either side received a message in the previous round.
    * If this is EdgeDirection.Both, sendMsg will only run on edges where *both* vertices received a message.
    * @return
    */
  def activeDirection(): EdgeDirection

  /**
    * Documentation from GraphX:
    * the message each vertex will receive at the first iteration
    * @return initial message
    */
  def initMessages(): A

  /**
    * Preprocesses the graph before running the pregel algorithm
    * @param temporalGraph graph to preprocess
    * @return preprocessed graph with the pregel state as vertex data
    */
  def preprocessGraph(temporalGraph: TemporalGraph): Graph[VD, ED]

  /**
    * Documentation from GraphX:
    * the user-defined vertex program which runs on each vertex and receives the inbound message and computes a new vertex value.
    * On the first iteration the vertex program is invoked on all vertices and is passed the default message.
    * On subsequent iterations the vertex program is only invoked on those vertices that receive messages.
    * @param vertexId id of the vertex
    * @param currentState current state of the vertex
    * @param message message, which have been merged by the mergeMessage function, and received by this vertex
    * @return new state of the vertex
    */
  def vertexProgram(vertexId: VertexId, currentState: VD, message: A): VD

  /**
    * Documentation from GraphX:
    * a user supplied function that is applied to out edges of vertices that received messages in the current iteration
    * @param triplet EdgeTriplet representing the edge that this function is applied to
    * @return Iterator of messages to send to the destination vertex
    */
  def sendMessage(triplet: EdgeTriplet[VD, ED]): Iterator[(VertexId, A)]

  /**
    * Documentation from GraphX:
    * a user supplied function that takes two incoming messages of type A and merges them into a single message of type A.
    * This function must be commutative and associative and ideally the size of A should not increase.
    * @param msg1 message 1
    * @param msg2 message 2
    * @return new merged message
    */
  def mergeMessage(msg1: A, msg2: A): A

  /**
    * Runs the pregel algorithm on the given graph
    * @param temporalGraph graph to run the algorithm on
    * @return graph with the result of the algorithm
    */
  def run(temporalGraph: TemporalGraph): Graph[VD, ED] = {
    val graph = preprocessGraph(temporalGraph)
    graph.pregel(
      initialMsg = initMessages(),
      maxIterations = maxIterations(),
      activeDirection = activeDirection()
    )(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )
  }

}
