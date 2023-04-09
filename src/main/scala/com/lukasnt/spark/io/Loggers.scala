package com.lukasnt.spark.io

import org.apache.log4j.{Level, LogManager, Logger}

object Loggers {

  val default: Logger = initDebugLogger()
  val root: Logger    = initRootLogger()

  private def initDebugLogger(): Logger = {
    val log = LogManager.getLogger("debugLogger")
    log.setLevel(Level.DEBUG)
    log.debug("Debug logger initialized")
    log
  }

  private def initRootLogger(): Logger = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    log.info("Root logger initialized")
    log
  }

}
