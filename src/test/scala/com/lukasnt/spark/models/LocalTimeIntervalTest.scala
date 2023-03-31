package com.lukasnt.spark.models

import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

import java.time.LocalTime

@Test
class LocalTimeIntervalTest {

  @Test
  def intervalBefore(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 1))
    val interval2 = new TemporalInterval(LocalTime.of(0, 2), LocalTime.of(0, 3))
    assertTrue(interval1.before(interval2))
  }

  @Test
  def intervalNotBefore(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 2))
    val interval2 = new TemporalInterval(LocalTime.of(0, 1), LocalTime.of(0, 3))
    assertFalse(interval1.before(interval2))
  }

  @Test
  def intervalEquals(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 5))
    val interval2 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 5))
    assertTrue(interval1.equals(interval2))
    assertTrue(interval2.equals(interval1))
  }

  @Test
  def intervalNotEquals(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 5))
    val interval2 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 6))
    assertFalse(interval1.equals(interval2))
    assertFalse(interval2.equals(interval1))
  }

  @Test
  def intervalMeets(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 1))
    val interval2 = new TemporalInterval(LocalTime.of(0, 1), LocalTime.of(0, 2))
    assertTrue(interval1.meets(interval2))
  }

  @Test
  def intervalOverlaps(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 3))
    val interval2 = new TemporalInterval(LocalTime.of(0, 2), LocalTime.of(0, 4))
    assertTrue(interval1.overlaps(interval2))
    assertTrue(interval1.overlaps(interval1))
  }

  @Test
  def intervalNotOverlaps(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 3))
    val interval2 = new TemporalInterval(LocalTime.of(0, 3), LocalTime.of(0, 4))
    assertFalse(interval1.overlaps(interval2))
    assertFalse(interval2.overlaps(interval1))
  }

  @Test
  def intervalDuring(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 1), LocalTime.of(0, 4))
    val interval2 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 5))
    assertTrue(interval1.during(interval2))
  }

  @Test
  def intervalNotDuring(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 1), LocalTime.of(0, 4))
    val interval2 = new TemporalInterval(LocalTime.of(0, 2), LocalTime.of(0, 5))
    assertFalse(interval1.during(interval2))
  }

  @Test
  def intervalStarts(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 1))
    val interval2 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 5))
    assertTrue(interval1.starts(interval2))
  }

  @Test
  def intervalNotStarts(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 2), LocalTime.of(0, 4))
    val interval2 = new TemporalInterval(LocalTime.of(0, 1), LocalTime.of(0, 5))
    assertFalse(interval1.starts(interval2))
  }

  @Test
  def intervalFinishes(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 3), LocalTime.of(0, 5))
    val interval2 = new TemporalInterval(LocalTime.of(0, 0), LocalTime.of(0, 5))
    assertTrue(interval1.finishes(interval2))
  }

  @Test
  def intervalNotFinishes(): Unit = {
    val interval1 = new TemporalInterval(LocalTime.of(0, 2), LocalTime.of(0, 5))
    val interval2 = new TemporalInterval(LocalTime.of(0, 2), LocalTime.of(0, 6))
    assertFalse(interval1.finishes(interval2))
  }
}
