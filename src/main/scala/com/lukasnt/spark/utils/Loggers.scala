package com.lukasnt.spark.utils

import org.apache.log4j.{Level, LogManager, Logger}

object Loggers {

  val default: Logger = initLogger()

  private def initLogger(): Logger = {
    val log = LogManager.getLogger("debugLogger")
    log.setLevel(Level.DEBUG)
    log.debug("Debug logger initialized")
    log
  }

}
