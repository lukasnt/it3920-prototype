package com.lukasnt.spark.queries

import com.lukasnt.spark.models.TemporalInterval
import com.lukasnt.spark.util.{IntervalStates, LengthWeightTable}
import org.junit.Assert.assertTrue
import org.junit.Test

import java.time.ZonedDateTime
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

@Test
class IntervalStatesTest {

  @Test
  def equals(): Unit = {
    val topK = 2
    val states1: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    val states2: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    assertTrue(states1 == states2)
  }

  @Test
  def topKEquals(): Unit = {
    val topK = 2
    val states1: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    val states2: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    assertTrue(states1 == states2)
  }

  @Test
  def notSortedButEquals(): Unit = {
    val topK = 2
    val states1: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    val states2: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    assertTrue(states1 == states2)
  }

  @Test
  def mergeStates(): Unit = {
    val topK = 3
    val states1: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    val states2: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    val resultStates: IntervalStates = IntervalStates(
      HashMap(
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:00:00Z"), ZonedDateTime.parse("2019-01-01T00:01:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        ),
        (
          TemporalInterval(ZonedDateTime.parse("2019-01-01T00:01:00Z"), ZonedDateTime.parse("2019-01-01T00:02:00Z")),
          LengthWeightTable(
            history = ArrayBuffer(),
            actives = ArrayBuffer(
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 0.23423f, 25123L),
              LengthWeightTable.Entry(1, 1.23423f, 25123L)
            ),
            topK
          )
        )
      )
    )

    assertTrue(states1.mergedStates(states2, topK) == resultStates)
  }
}
