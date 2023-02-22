package com.lukasnt.spark

import org.junit.Assert.assertTrue
import org.junit.Test

import java.time.LocalDateTime

class LocalDateTimeIntervalTest {

  @Test
  def intervalBefore(): Unit = {
    val interval1 = new TemporalInterval(LocalDateTime.of(0, 1, 1, 0, 0),
                                         LocalDateTime.of(0, 1, 1, 0, 1))
    val interval2 = new TemporalInterval(LocalDateTime.of(1, 1, 1, 0, 2),
                                         LocalDateTime.of(1, 1, 1, 0, 3))
    assertTrue(interval1.before(interval2))
  }

  @Test
  def intervalEquals(): Unit = {
    val interval1 = new TemporalInterval(LocalDateTime.of(1, 1, 1, 1, 1),
                                         LocalDateTime.of(2, 2, 2, 2, 2))
    val interval2 = new TemporalInterval(LocalDateTime.of(1, 1, 1, 1, 1),
                                         LocalDateTime.of(2, 2, 2, 2, 2))
    assertTrue(interval1.equals(interval2))
    assertTrue(interval2.equals(interval1))
  }

  @Test
  def intervalOverlaps(): Unit = {
    val interval1 = new TemporalInterval(LocalDateTime.of(1, 1, 1, 1, 1),
                                         LocalDateTime.of(2, 2, 2, 2, 3))
    val interval2 = new TemporalInterval(LocalDateTime.of(2, 2, 2, 2, 2),
                                         LocalDateTime.of(3, 3, 3, 3, 3))
    assertTrue(interval1.overlaps(interval2))
  }
}
