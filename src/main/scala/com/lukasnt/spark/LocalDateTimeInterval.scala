package com.lukasnt.spark

import java.time.LocalDateTime

/**
 * The LocalDateTime instance of the TemporalInterval class.
 * @param startTime start time of the interval in LocalDateTime format
 * @param endTime end time of the interval in LocalDateTime format
 */
class LocalDateTimeInterval(override val startTime: LocalDateTime, override val endTime: LocalDateTime)
  extends TemporalInterval[LocalDateTime](startTime, endTime);
