package com.lukasnt.spark.experiments

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.QueryResult
import org.apache.spark.sql.SparkSession

class Experiment {

  private var _name: String    = "Experiment"
  private var _variableSet     = new VariableSet()
  private var _variableOrder   = Experiment.VariableOrder.Ascending
  private var _runsPerVariable = 10
  private var _maxVariables    = 5

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
          println()
          println("=====================================")
          println(s"Graph Loader: ${graphLoader.getClass.getSimpleName}")
          println("-------------------------------------")
          println(s"Executor: ${executor.getClass.getSimpleName}")
          println("-------------------------------------")
          println("Query:")
          println(query)
          println("-------------------------------------")
          println("Loading graph...")
          val temporalGraph: TemporalGraph = graphLoader.load(_sparkSession.sparkContext)
          println("-------------------------------------")
          println("Starting query execution...")
          val queryResult: QueryResult = executor.execute(query, temporalGraph)
          println("-------------------------------------")
          println("Table results:")
          queryResult.asDataFrame(_sparkSession.sqlContext).show(100, truncate = false)
          println("-------------------------------------")
          println("Raw results:")
          println(queryResult)
          println("=====================================")
          println()
        }
    }

  }

}

object Experiment {

  def builder() = new ExperimentBuilder()

  class ExperimentBuilder {

    private val experiment = new Experiment()

    def build(): Experiment = {
      experiment
    }

    def withName(name: String): ExperimentBuilder = {
      experiment._name = name
      this
    }

    def withRunsPerVariable(runsPerVariable: Int): ExperimentBuilder = {
      experiment._runsPerVariable = runsPerVariable
      this
    }

    def withVariableSet(variableSet: VariableSet): ExperimentBuilder = {
      experiment._variableSet = variableSet
      this
    }

    def withVariableOrder(variableOrder: Experiment.VariableOrder.Value): ExperimentBuilder = {
      experiment._variableOrder = variableOrder
      this
    }

    def withMaxVariables(maxVariables: Int): ExperimentBuilder = {
      experiment._maxVariables = maxVariables
      this
    }

    def withSparkSession(sparkSession: SparkSession): ExperimentBuilder = {
      experiment._sparkSession = sparkSession
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
