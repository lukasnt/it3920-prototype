package com.lukasnt.spark.experiments

import com.lukasnt.spark.executors.{ParameterQueryExecutor, SparkQueryExecutor}
import com.lukasnt.spark.io.CSVUtils.SnbCSVProperties
import com.lukasnt.spark.io.{SNBLoader, SingleLocalCSV, TemporalGraphLoader}
import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.queries.ParameterQuery
import org.apache.spark.SparkContext

import java.time.ZonedDateTime

class Experiment {

  private var _name: String    = "Experiment"
  private var _runsPerVariable = 10
  private var _variableSet     = new VariableSet()
  private var _variableOrder   = Experiment.VariableOrder.Ascending
  private var _maxVariables    = 5

  private var _sparkContext: SparkContext       = SparkContext.getOrCreate()
  private var _executor: ParameterQueryExecutor = SparkQueryExecutor()
  private var _graphLoader: TemporalGraphLoader[ZonedDateTime] =
    new SNBLoader("/", new SingleLocalCSV(SnbCSVProperties))

  def name: String                                    = this._name
  def runsPerVariable: Int                            = this._runsPerVariable
  def variableSet: VariableSet                        = this._variableSet
  def variableOrder: Experiment.VariableOrder.Value   = this._variableOrder
  def maxVariables: Int                               = this._maxVariables
  def sparkContext: SparkContext                      = this._sparkContext
  def executor: ParameterQueryExecutor                = this._executor
  def graphLoader: TemporalGraphLoader[ZonedDateTime] = this._graphLoader

  def run(runsPerVariable: Int = _runsPerVariable,
          variableOrder: Experiment.VariableOrder.Value = _variableOrder,
          maxVariables: Int = _maxVariables): Unit = {
    val queries: List[ParameterQuery] = variableOrder match {
      case Experiment.VariableOrder.Ascending  => _variableSet.ascendingQueries
      case Experiment.VariableOrder.Descending => _variableSet.descendingQueries
      case Experiment.VariableOrder.Shuffled   => _variableSet.shuffledQueries
      case _                                   => _variableSet.ascendingQueries
    }

    val temporalGraph: TemporalGraph = _graphLoader.load(_sparkContext)

    val queriesToRun: List[ParameterQuery] = queries.take(maxVariables)
    queriesToRun.foreach(query => {
      for (i <- 1 to runsPerVariable) {
        println(s"Running query: $query")
        _executor.execute(query, temporalGraph)
      }
    })

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

    def withSparkContext(sparkContext: SparkContext): ExperimentBuilder = {
      experiment._sparkContext = sparkContext
      this
    }

    def withExecutor(executor: ParameterQueryExecutor): ExperimentBuilder = {
      experiment._executor = executor
      this
    }

    def withGraphLoader(graphLoader: TemporalGraphLoader[ZonedDateTime]): ExperimentBuilder = {
      experiment._graphLoader = graphLoader
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
