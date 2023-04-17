package com.lukasnt.spark.experiments

import com.lukasnt.spark.executors.ParameterQueryExecutor
import com.lukasnt.spark.io.TemporalGraphLoader
import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.{ParameterQuery, QueryResult}
import org.apache.spark.sql.SparkSession

import java.time.ZonedDateTime

class Experiment {

  var results: List[QueryExecutionResult] = List()
  private var _name: String               = "Experiment"
  private var _variableSet                = new VariableSet()
  private var _variableOrder              = Experiment.VariableOrder.Ascending
  private var _runsPerVariable            = 10
  private var _maxVariables               = 5
  private var _saveResults                = true
  private var _printEnabled               = true
  private var _sparkSession: SparkSession = SparkSession.builder().getOrCreate()

  def name: String                                  = this._name
  def runsPerVariable: Int                          = this._runsPerVariable
  def variableSet: VariableSet                      = this._variableSet
  def variableOrder: Experiment.VariableOrder.Value = this._variableOrder
  def maxVariables: Int                             = this._maxVariables
  def sparkSession: SparkSession                    = this._sparkSession

  def run(runsPerVariable: Int = _runsPerVariable,
          variableOrder: Experiment.VariableOrder.Value = _variableOrder,
          maxVariables: Int = _maxVariables): Unit = {

    val queries: List[VariableSet.QueryExecutionSet] = variableOrder match {
      case Experiment.VariableOrder.Ascending  => _variableSet.ascendingQueries
      case Experiment.VariableOrder.Descending => _variableSet.descendingQueries
      case Experiment.VariableOrder.Shuffled   => _variableSet.shuffledQueries
      case _                                   => _variableSet.ascendingQueries
    }

    val queriesToRun: List[VariableSet.QueryExecutionSet] = queries.take(maxVariables)
    queriesToRun.foreach {
      case VariableSet.QueryExecutionSet(query, graphLoader, executor) =>
        (1 to runsPerVariable).foreach { _ =>
          if (_printEnabled) {
            // Print experiment info
            clearSparkResources()
            printBorder()
            printExperimentInfo(graphLoader, executor, query)

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

          if (_saveResults) {
            this.results = this.results :+ QueryExecutionResult(
              query = query,
              graphName = graphLoader.getClass.getSimpleName,
              executorName = executor.getClass.getSimpleName,
              queryResult = queryResult,
              executionTime = queryExecutionTime
            )
          }
        }
    }

  }

  private def printSparkStats(): Unit = {
    println(s"Spark Memory Status: ${_sparkSession.sparkContext.getExecutorMemoryStatus}")
    println(s"Spark RDD Storage Info:")
    println(_sparkSession.sparkContext.getRDDStorageInfo.mkString("\n"))
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

  private def printExperimentInfo(graphLoader: TemporalGraphLoader[ZonedDateTime],
                                  executor: ParameterQueryExecutor,
                                  query: ParameterQuery): Unit = {
    println(s"Graph Loader: ${graphLoader.getClass.getSimpleName}")
    println(s"Executor: ${executor.getClass.getSimpleName}")
    println("Query:")
    println(query)
    printSparkStats()
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

    def withPrintEnabled(printResults: Boolean): Builder = {
      experiment._printEnabled = printResults
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
