package com.lukasnt.spark

import com.lukasnt.spark.examples.SimpleParameterQueries
import com.lukasnt.spark.executors.ParameterQueryExecutor
import com.lukasnt.spark.experiments.{Experiment, IntPairConverter, VariableOrderConverter, VariableSet}
import com.lukasnt.spark.io.SNBLoader
import com.lukasnt.spark.models.TemporalPathType
import com.lukasnt.spark.queries.ParameterQuery
import org.apache.spark.sql.SparkSession
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

import java.util.concurrent.Callable

/**
  * @author ${user.name}
  */
@Command(name = "experiments",
         version = Array("Experiments 1.0"),
         description = Array("Spark experiments"),
         mixinStandardHelpOptions = true,
         subcommandsRepeatable = true)
class App extends Callable[Int] {

  @Option(names = Array("-hdfs", "--hdfs-root-dir"), description = Array("HDFS root directory"))
  private var hdfsRootDir: String = System.getenv("HDFS_ROOT_DIR")

  @Command
  def experiment(
      @Option(names = Array("-n", "--name"), description = Array("Name of the experiment"), defaultValue = "Experiment")
      name: String,
      @Option(names = Array("-m", "--max-variables"),
              description = Array("Maximum number of variables run"),
              defaultValue = "1")
      maxVariables: Int,
      @Option(names = Array("-r", "--runs-per-variable"),
              description = Array("Number of runs per variable"),
              defaultValue = "1")
      runsPerVariable: Int,
      @Option(names = Array("-o", "--variable-order"),
              description = Array("Variable order"),
              converter = Array(classOf[VariableOrderConverter]),
              defaultValue = "Ascending")
      variableOrder: Experiment.VariableOrder.Value,
      @Option(names = Array("-s", "--save-results"),
              description = Array("Save results in between runs that can be viewed later"),
              defaultValue = "false")
      saveResults: Boolean,
      @Option(names = Array("-p", "--print-enabled"),
              description = Array("Enable printing of results and info"),
              defaultValue = "true")
      printEnabled: Boolean,
      @Option(names = Array("-l", "--log-enabled"),
              description = Array("Enable logging of results and info"),
              defaultValue = "true")
      logEnabled: Boolean,
      @Option(names = Array("-qp", "--query-preset"),
              description = Array("Query preset"),
              defaultValue = "city-interaction-duration")
      queryPreset: String = "city-interaction-duration",
      @Option(names = Array("-ev", "--executor-variables"),
              description = Array("Executor variables"),
              defaultValue = "spark")
      executorVariables: Array[String] = Array("spark"),
      @Option(names = Array("-gv", "--graph-variables"),
              description = Array("Graph dataset variables"),
              defaultValue = "sf1")
      graphVariables: Array[String] = Array("sf1"),
      @Option(names = Array("-tpv", "--temporal-path-variables"), description = Array("Temporal path variables"))
      temporalPathVariables: Array[String] = Array(),
      @Option(
        names = Array("-lrv", "--length-range-variables"),
        description = Array("Length range variables"),
        converter = Array(classOf[IntPairConverter])
      )
      lengthRangeVariables: Array[(Int, Int)] = Array(),
      @Option(names = Array("-tkv", "--top-k-variables"), description = Array("number of top k results"))
      topKVariables: Array[Int] = Array()
  ): Unit = {
    println(
      s"Experiment: $name\n" +
        s"maxVariables: $maxVariables\n" +
        s"runsPerVariable: $runsPerVariable\n" +
        s"variableOrder: $variableOrder\n" +
        s"saveResults: $saveResults\n" +
        s"printEnabled: $printEnabled\n" +
        s"logEnabled: $logEnabled\n" +
        s"queryPreset: $queryPreset\n" +
        s"executorVariables: ${if (executorVariables != null) executorVariables.mkString(", ")}\n" +
        s"graphVariables: ${if (graphVariables != null) graphVariables.mkString(", ")}\n" +
        s"temporalPathVariables: ${if (temporalPathVariables != null) temporalPathVariables.mkString(", ")}\n" +
        s"lengthRangeVariables: ${if (lengthRangeVariables != null) lengthRangeVariables.mkString(", ")}\n" +
        s"topKVariables: ${if (topKVariables != null) topKVariables.mkString(", ")}"
    )

    val spark                                = SparkSession.builder().getOrCreate()
    val parameterQueryPreset: ParameterQuery = ParameterQuery.getByName(queryPreset)
    Experiment
      .builder()
      .withName(name)
      .withSparkSession(spark)
      .withMaxVariables(maxVariables)
      .withRunsPerVariable(runsPerVariable)
      .withVariableOrder(Experiment.VariableOrder.Ascending)
      .withSaveResults(saveResults)
      .withPrintEnabled(printEnabled)
      .withVariableSet(
        VariableSet
          .builder()
          .withExecutorVariables(executorVariables.map(ParameterQueryExecutor.getByName).toList)
          .withGraphLoaderVariables(graphVariables.map(SNBLoader.getByName(_, spark.sqlContext, hdfsRootDir)).toList)
          .fromParameterQuery(parameterQueryPreset)
          .withTemporalPathTypeVariables(
            if (temporalPathVariables != null) temporalPathVariables.map(TemporalPathType.getByName).toList
            else List(parameterQueryPreset.temporalPathType)
          )
          .withTopKVariables(
            if (topKVariables != null) topKVariables.toList
            else List(parameterQueryPreset.topK)
          )
          .withLengthRangeVariables(
            if (lengthRangeVariables != null) lengthRangeVariables.toList
            else List((parameterQueryPreset.minLength, parameterQueryPreset.maxLength))
          )
          .build())
      .build()
      .run()
  }

  @Command
  def test(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val genderDurationSf0_003 = Experiment
      .builder()
      .withName("gender-interaction-duration-sf0_003")
      .withSparkSession(spark)
      .withMaxVariables(8)
      .withRunsPerVariable(1)
      .withVariableOrder(Experiment.VariableOrder.Ascending)
      .withSaveResults(true)
      .withPrintEnabled(true)
      .withVariableSet(
        VariableSet
          .builder()
          .fromParameterQuery(SimpleParameterQueries.genderDurationPaths())
          .withTopKVariables(List(3, 25))
          .withLengthRangeVariables(List((1, 2), (4, 5)))
          .withExecutorVariables(List("spark", "serial").map(ParameterQueryExecutor.getByName))
          .withGraphLoaderVariables(List(SNBLoader.getByName("sf0_003", spark.sqlContext, hdfsRootDir)))
          .build())
      .build()
    genderDurationSf0_003.run()

    genderDurationSf0_003.results.groupBy(_.query.toString).foreach {
      case (_, results) =>
        println()
        results.head.printComparison(results.tail.head)
    }

    val interactionDurationSf1 = Experiment
      .builder()
      .withName("city-interaction-duration-sf1")
      .withSparkSession(spark)
      .withMaxVariables(2)
      .withRunsPerVariable(1)
      .withVariableOrder(Experiment.VariableOrder.Ascending)
      .withSaveResults(true)
      .withPrintEnabled(false)
      .withVariableSet(
        VariableSet
          .builder()
          .fromParameterQuery(SimpleParameterQueries.interactionPaths())
          .withTopKVariables(List(10))
          .withLengthRangeVariables(List((2, 3)))
          .withExecutorVariables(List("spark", "serial").map(ParameterQueryExecutor.getByName))
          .withGraphLoaderVariables(List(SNBLoader.getByName("sf1", spark.sqlContext, hdfsRootDir)))
          .build())
      .build()
    interactionDurationSf1.run()

    interactionDurationSf1.results.groupBy(_.query.toString).foreach {
      case (_, results) =>
        println()
        results.head.printComparison(results.tail.head)
    }
  }

  override def call(): Int = {
    0
  }

}

object App {

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    new CommandLine(new App()).execute(args: _*)
  }

}
