package com.lukasnt.spark

import java.time.LocalDateTime

class LocalDateTimeInterval(override val startTime: LocalDateTime, override val endTime: LocalDateTime)
  extends TemporalInterval[LocalDateTime](startTime, endTime);
