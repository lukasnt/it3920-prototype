package com.lukasnt.spark.experiments

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.QueryResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import java.time.{LocalDateTime, ZoneOffset}

class Experiment {

  var results: List[QueryExecutionResult] = List()
  private var _dateStarted: LocalDateTime = LocalDateTime.now()
  private var _name: String               = "Experiment"
  private var _variableSet                = new VariableSet()
  private var _variableOrder              = Experiment.VariableOrder.Ascending
  private var _runsPerVariable            = 10
  private var _maxVariables               = 5
  private var _saveResults                = true
  private var _writeResults               = true
  private var _printEnabled               = true
  private var _logEnabled                 = true
  private var _resultDir: String          = "results"
  private var _sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  def runsPerVariable: Int = this._runsPerVariable

  def variableSet: VariableSet = this._variableSet

  def variableOrder: Experiment.VariableOrder.Value = this._variableOrder

  def maxVariables: Int = this._maxVariables

  def run(runsPerVariable: Int = _runsPerVariable,
          variableOrder: Experiment.VariableOrder.Value = _variableOrder,
          maxVariables: Int = _maxVariables): Unit = {
    if (_writeResults) {
      // Initialize result file
      initResultFile()
    }

    val queries: List[VariableSet.QueryExecutionSet] = variableOrder match {
      case Experiment.VariableOrder.Ascending  => _variableSet.ascendingQueries
      case Experiment.VariableOrder.Descending => _variableSet.descendingQueries
      case Experiment.VariableOrder.Shuffled   => _variableSet.shuffledQueries
      case _                                   => _variableSet.ascendingQueries
    }

    val queriesToRun: List[VariableSet.QueryExecutionSet] = queries.take(maxVariables)
    queriesToRun.foreach {
      case VariableSet.QueryExecutionSet(query,
                                         queryName,
                                         graphName,
                                         graphSize,
                                         graphLoader,
                                         executor,
                                         executorCount) =>
        (1 to runsPerVariable).foreach { runNumber =>
          if (_printEnabled) {
            // Print experiment info
            clearSparkResources()
            printBorder()
            printExperimentInfo(
              VariableSet.QueryExecutionSet(query,
                                            queryName,
                                            graphName,
                                            graphSize,
                                            graphLoader,
                                            executor,
                                            executorCount)
            )

            // Load Graph init print
            println("-------------------------------------")
            println("Loading graph...")
          }

          // Load graph
          val loadingStartTime             = System.currentTimeMillis()
          val temporalGraph: TemporalGraph = graphLoader.load(_sparkSession.sparkContext)

          if (_printEnabled) {
            // Print loading time
            println(s"Graph loaded in ${System.currentTimeMillis() - loadingStartTime} ms")
            printSparkStats()

            // Query Execution init print
            println("-------------------------------------")
            println("Starting query execution...")
          }

          // Execute query
          val queryStartTime           = System.currentTimeMillis()
          val queryResult: QueryResult = executor.execute(query, temporalGraph)
          val queryExecutionTime       = System.currentTimeMillis() - queryStartTime

          if (_printEnabled) {
            // Print execution time
            println(s"Query executed in $queryExecutionTime ms")
            printSparkStats()

            // Print results
            printResult(queryResult)
            clearSparkResources()
            printBorder()
          }

          val result = QueryExecutionResult(
            runNumber = runNumber,
            queryName = queryName,
            query = query,
            graphName = graphName,
            graphSize = graphSize,
            sparkExecutorInstances = executorCount,
            executorName = executor.getClass.getSimpleName,
            queryResult = queryResult,
            executionTime = queryExecutionTime
          )

          if (_saveResults) this.results = this.results :+ result
          if (_writeResults) appendResultToFile(result)
        }
    }

  }

  private def clearSparkResources(): Unit = {
    // Remove all RDDs from memory and disk
    _sparkSession.sparkContext.getPersistentRDDs.foreach {
      case (_, rdd) => rdd.unpersist()
    }

    // Clear Spark Cache
    _sparkSession.sharedState.cacheManager.clearCache()
    _sparkSession.sqlContext.clearCache()

    // Clear Spark session and context state
    _sparkSession.sessionState.catalog.reset()
    _sparkSession.sparkContext.clearJobGroup()
    _sparkSession.sparkContext.clearCallSite()
  }

  private def printExperimentInfo(variableSet: VariableSet.QueryExecutionSet): Unit = {
    println(s"Graph Loader Class: ${variableSet.graphLoader.getClass.getSimpleName}")
    println(s"Executor (Algorithm) Class: ${variableSet.executor.getClass.getSimpleName}")
    println(s"Variables: ------------------------")
    println(s"Query: ${variableSet.queryName}")
    println(s"Min Length: ${variableSet.query.minLength}")
    println(s"Max Length: ${variableSet.query.maxLength}")
    println(s"TopK: ${variableSet.query.topK}")
    println(s"Graph Name: ${variableSet.graphName}")
    println(s"Graph Size: ${variableSet.graphSize}")
    println(s"Spark Executor Count: ${variableSet.sparkExecutorCount}")
    printSparkStats()
  }

  private def printSparkStats(): Unit = {
    println(s"Spark Memory Status: ${_sparkSession.sparkContext.getExecutorMemoryStatus}")
    println(s"Spark RDD Storage Info:")
    println(_sparkSession.sparkContext.getRDDStorageInfo.mkString("\n"))
  }

  private def printBorder(): Unit = {
    println()
    println("=====================================")
  }

  private def printResult(queryResult: QueryResult): Unit = {
    println("-------------------------------------")
    println("Table results:")
    queryResult.asDataFrame(_sparkSession.sqlContext).show(100, truncate = false)
    println("-------------------------------------")
    println("Raw results:")
    println(queryResult)
  }

  private def initResultFile(): Unit = {
    _dateStarted = LocalDateTime.now()
    val sqlContext: SQLContext              = _sparkSession.sqlContext
    val emptyRDD: RDD[QueryExecutionResult] = _sparkSession.sparkContext.emptyRDD
    sqlContext
      .createDataFrame(new java.util.ArrayList[Row](), QueryExecutionResult.infoResultsAsDataFrameSchema())
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(s"${_resultDir}/$getFileName")
  }

  private def getFileName: String = {
    s"${_name}-${_dateStarted.toLocalDate}${_dateStarted.toInstant(ZoneOffset.UTC).toEpochMilli}.csv"
  }

  private def appendResultToFile(queryExecutionResult: QueryExecutionResult): Unit = {
    val sqlContext: SQLContext = _sparkSession.sqlContext
    sqlContext
      .createDataFrame(sqlContext.sparkContext.parallelize(List(queryExecutionResult.infoResultsAsDataFrame())),
                       QueryExecutionResult.infoResultsAsDataFrameSchema())
      .write
      .mode("append")
      .csv(s"${_resultDir}/$getFileName")
  }

  def sparkSession: SparkSession = this._sparkSession

  def name: String = this._name
}

object Experiment {

  def builder() = new Builder()

  class Builder {

    private val experiment = new Experiment()

    def build(): Experiment = {
      experiment
    }

    def withName(name: String): Builder = {
      experiment._name = name
      this
    }

    def withRunsPerVariable(runsPerVariable: Int): Builder = {
      experiment._runsPerVariable = runsPerVariable
      this
    }

    def withVariableSet(variableSet: VariableSet): Builder = {
      experiment._variableSet = variableSet
      this
    }

    def withVariableOrder(variableOrder: Experiment.VariableOrder.Value): Builder = {
      experiment._variableOrder = variableOrder
      this
    }

    def withMaxVariables(maxVariables: Int): Builder = {
      experiment._maxVariables = maxVariables
      this
    }

    def withSparkSession(sparkSession: SparkSession): Builder = {
      experiment._sparkSession = sparkSession
      this
    }

    def withSaveResults(saveResults: Boolean): Builder = {
      experiment._saveResults = saveResults
      this
    }

    def withWriteResults(writeResults: Boolean): Builder = {
      experiment._writeResults = writeResults
      this
    }

    def withPrintEnabled(printResults: Boolean): Builder = {
      experiment._printEnabled = printResults
      this
    }

    def withLogEnabled(logEnabled: Boolean): Builder = {
      experiment._logEnabled = logEnabled
      this
    }

    def withResultDir(resultDir: String): Builder = {
      experiment._resultDir = resultDir
      this
    }

  }

  object VariableOrder extends Enumeration {
    val Ascending, Descending, Shuffled = Value
  }

  object RunOrder extends Enumeration {
    val Sequential, Interleaved, RandomBatch = Value
  }

}
