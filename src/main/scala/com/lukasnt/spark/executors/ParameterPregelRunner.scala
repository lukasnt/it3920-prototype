package com.lukasnt.spark.executors

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.models.Types.{PregelGraph, PregelVertex, TemporalGraph}
import com.lukasnt.spark.queries._
import com.lukasnt.spark.utils.Loggers
import org.apache.spark.graphx.EdgeDirection

import scala.util.Random

object ParameterPregelRunner {

  def run(parameterQuery: ParameterQuery, temporalGraph: TemporalGraph): PregelGraph = {
    // Create the init states beforehand as Queries are not serializable
    val initConstState = ConstState.builder().withSeqNum(0).build()
    val initMessages   = IntervalMessage()

    // Map to temporal pregel graph
    val pregelGraph = temporalGraph.mapVertices(
      (_, _) =>
        PregelVertex(
          initConstState,
          IntervalsState(
            List(
              IntervalsState.Entry(TemporalInterval(),
                                   LengthWeightTable(
                                     List(
                                       LengthWeightTable.Entry(0, Random.nextFloat(), 0)
                                     )
                                   ))
            )
          )
      )
    )

    // Run Pregel
    val result = pregelGraph.pregel[IntervalMessage](
      initMessages,
      100,
      EdgeDirection.Out
    )(
      // Vertex Program
      (id, attr, msg) => {
        Loggers.default.debug(
          s"id: $id, superstep: ${attr.constState.superstep}, firstEntry: ${attr.intervalsState.intervalData.head}"
        )

        val newConstState = ConstState
          .builder()
          .fromState(attr.constState)
          .incSuperstep()
          .applyPathCostUpdate(attr.constState.pathCost - Random.nextFloat())
          .build()
        val newIntervalsState = attr.intervalsState
          .updateWithEntry(
            IntervalsState.Entry(
              msg.interval,
              msg.lengthWeightTable.updateWithEntry(
                LengthWeightTable.Entry(msg.length, Random.nextFloat(), id)
              )
            )
          )

        PregelVertex(newConstState, newIntervalsState)
      },
      // Send Message
      triplet => {
        val length   = triplet.srcAttr.constState.superstep
        val interval = triplet.attr.interval

        Loggers.default.debug(
          s"srcId: ${triplet.srcId}, " +
            s"srcSuperstep: ${triplet.srcAttr.constState.superstep}, " +
            s"length: $length, " +
            s"dstSuperstep: ${triplet.dstAttr.constState.superstep}")

        Iterator((triplet.srcId, IntervalMessage(interval, length)))
      },
      // Merge Message
      (a, b) => {
        b
      }
    )

    result
  }

}
