package com.lukasnt.spark

import java.time.LocalTime

/**
 * The LocalTime instance of the TemporalInterval class.
 * @param startTime start time of the interval in LocalTime format
 * @param endTime end time of the interval in LocalTime format
 */
class LocalTimeInterval(override val startTime: LocalTime, override val endTime: LocalTime)
  extends TemporalInterval[LocalTime](startTime, endTime);
