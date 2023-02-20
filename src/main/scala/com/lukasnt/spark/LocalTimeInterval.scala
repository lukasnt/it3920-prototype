package com.lukasnt.spark

import java.time.LocalTime

class LocalTimeInterval(override val startTime: LocalTime, override val endTime: LocalTime)
  extends TemporalInterval[LocalTime](startTime, endTime);
