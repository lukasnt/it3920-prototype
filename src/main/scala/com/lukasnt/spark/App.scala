package com.lukasnt.spark

import com.lukasnt.spark.executors.ParameterQueryExecutor
import com.lukasnt.spark.experiments._
import com.lukasnt.spark.io.{SNBLoader, SparkObjectWriter, TemporalGraphLoader}
import com.lukasnt.spark.models.TemporalPathType
import com.lukasnt.spark.queries.ParameterQuery
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.sql.SparkSession
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

import java.time.ZonedDateTime
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
      @Option(names = Array("-w", "--write-enabled"),
              description = Array("Enable writing of results and info to file"),
              defaultValue = "true")
      writeEnabled: Boolean,
      @Option(names = Array("-qp", "--query-preset"),
              description = Array("Query preset"),
              defaultValue = "city-interaction-duration")
      queryPreset: String = "city-interaction-duration",
      @Option(names = Array("-ev", "--executor-variables"),
              description = Array("Executor variables"),
              defaultValue = "spark")
      executorVariables: Array[String] = Array("spark"),
      @Option(names = Array("-gnv", "--graph-variables"),
              description = Array("Graph name variables"),
              defaultValue = "interaction")
      graphVariables: Array[String] = Array("interaction"),
      @Option(names = Array("-gsv", "--graph-size-variables"),
              description = Array("Graph dataset size variables"),
              defaultValue = "sf1")
      graphSizeVariables: Array[String] = Array("sf1"),
      @Option(names = Array("-ecv", "--executor-count-variables"),
              description = Array("Executor count variables"),
              defaultValue = "4")
      executorCountVariables: Array[Int] = Array(4),
      @Option(names = Array("-psv", "--partition-strategy-variables"),
              description = Array("Partition strategy variables"),
              defaultValue = "RandomVertexCut")
      partitionStrategyVariables: Array[String] = Array("RandomVertexCut"),
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
        s"writeEnabled: $writeEnabled\n" +
        s"queryPreset: $queryPreset\n" +
        s"executorVariables: ${executorVariables.mkString(", ")}\n" +
        s"graphVariables: ${graphVariables.mkString(", ")}\n" +
        s"graphSizeVariables: ${graphSizeVariables.mkString(", ")}\n" +
        s"executorCountVariables: ${executorCountVariables.mkString(", ")}\n" +
        s"partitionStrategyVariables: ${partitionStrategyVariables.mkString(", ")}\n" +
        s"temporalPathVariables: ${if (temporalPathVariables != null) temporalPathVariables.mkString(", ")}\n" +
        s"lengthRangeVariables: ${if (lengthRangeVariables != null) lengthRangeVariables.mkString(", ")}\n" +
        s"topKVariables: ${if (topKVariables != null) topKVariables.mkString(", ")}"
    )

    val spark                                = SparkSession.builder().appName(name).getOrCreate()
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
      .withLogEnabled(logEnabled)
      .withWriteResults(writeEnabled)
      .withResultDir(s"$hdfsRootDir/results")
      .withVariableSet(
        VariableSet
          .builder()
          .withExecutorVariables(executorVariables.map(ParameterQueryExecutor.getByName).toList)
          .withPartitionStrategyVariables(partitionStrategyVariables.map(PartitionStrategy.fromString).toList)
          .withSparkExecutorCountVariables(executorCountVariables.toList)
          .fromParameterQuery(parameterQueryPreset, queryPreset)
          .withGraphLoaderVariables(
            (
              for {
                graphName <- graphVariables
                graphSize <- graphSizeVariables
              } yield (graphName, graphSize, GraphLoaders.getByName(graphName, graphSize, spark, hdfsRootDir))
            ).toList
          )
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
          .build()
      )
      .build()
      .run()
  }

  @Command
  def test(): Unit = {
    val spark = SparkSession.builder().appName("Test").getOrCreate()
    val genderDurationSf0_003 = Experiment
      .builder()
      .withName("gender-interaction-duration-sf0_003")
      .withSparkSession(spark)
      .withMaxVariables(8)
      .withRunsPerVariable(1)
      .withVariableOrder(Experiment.VariableOrder.Ascending)
      .withSaveResults(true)
      .withPrintEnabled(true)
      .withLogEnabled(false)
      .withWriteResults(false)
      .withVariableSet(
        VariableSet
          .builder()
          .fromParameterQuery(ParameterQuery.genderNumInteractionPaths(), "gender-num-interaction")
          .withTopKVariables(List(3, 25))
          .withLengthRangeVariables(List((1, 2), (4, 5)))
          .withExecutorVariables(List("spark", "serial").map(ParameterQueryExecutor.getByName))
          .withGraphLoaderVariables(List(
            ("interaction", "sf0_003", GraphLoaders.getByName("interaction", "sf0_003", spark, hdfsRootDir))))
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
      .withPrintEnabled(true)
      .withLogEnabled(false)
      .withWriteResults(false)
      .withVariableSet(
        VariableSet
          .builder()
          .fromParameterQuery(ParameterQuery.cityInteractionDurationPaths(), "city-interaction-duration")
          .withTopKVariables(List(10))
          .withLengthRangeVariables(List((2, 3)))
          .withExecutorVariables(List("spark", "serial").map(ParameterQueryExecutor.getByName))
          .withGraphLoaderVariables(List(("raw", "sf1", SNBLoader.getByName("sf1", spark.sqlContext, hdfsRootDir))))
          .build()
      )
      .build()
    interactionDurationSf1.run()

    interactionDurationSf1.results.groupBy(_.query.toString).foreach {
      case (_, results) =>
        println()
        results.head.printComparison(results.tail.head)
    }
  }

  @Command
  def preprocess(
      @Option(names = Array("-gn", "--graph-name"), description = Array("raw graph name"))
      graphName: String = "raw",
      @Option(names = Array("-gs", "--graph-size"), description = Array("graph size name"))
      graphSizes: Array[String] = Array("sf1"),
      @Option(names = Array("-snb", "--snb-raw"), description = Array("use raw snb dataset"))
      snbRaw: Boolean = false,
      @Option(names = Array("-cb", "--citi-bike-raw"), description = Array("use raw citi bike dataset"))
      citiBikeRaw: Boolean = false,
      @Option(names = Array("-pl", "--preprocess-loaders"), description = Array("preprocessor name"))
      preprocessLoaders: Array[String] = Array(),
      @Option(names = Array("-o", "--output-dir"), description = Array("output directory"))
      outputDir: String = s"$hdfsRootDir"
  ): Unit = {
    val spark = SparkSession.builder().appName("Preprocess").getOrCreate()
    val graphLoaders: Array[(String, TemporalGraphLoader[ZonedDateTime])] = if (snbRaw) {
      graphSizes.map(
        sizeName => (sizeName, GraphLoaders.getByName(graphName, sizeName, spark, hdfsRootDir, rawSNB = snbRaw))
      )
    } else {
      graphSizes.map(
        sizeName => (sizeName, GraphLoaders.getByName(graphName, sizeName, spark, hdfsRootDir))
      )
    }
    val outputDirString: String = if (outputDir == null) s"$hdfsRootDir" else outputDir
    graphLoaders.foreach {
      case (rawGraphName, graphLoader) =>
        preprocessLoaders
          .map(name => (name, PreprocessLoaders.getByName(name, graphLoader)))
          .foreach {
            case (preprocessName, preprocessLoader) =>
              println(s"Preprocessing $rawGraphName with $preprocessName")
              SparkObjectWriter.write(
                graph = preprocessLoader.load(spark.sparkContext),
                path = s"$outputDirString/preprocessed/$preprocessName/$rawGraphName"
              )
              println(s"Finished writing to $outputDirString/preprocessed/$preprocessName/$rawGraphName")
          }
    }

  }

  @Command
  def printCSV(
      @Option(names = Array("-f", "--filename"), description = Array("filename"))
      filename: String,
      @Option(names = Array("-c", "--columns"), description = Array("columns"))
      columns: Array[String] = Array()
  ): Unit = {
    val columnNames =
      if (columns == null || columns.isEmpty) QueryExecutionResult.infoResultsAsDataFrameSchema().names
      else columns
    val spark = SparkSession.builder().getOrCreate()
    println(columnNames.mkString(","))
    spark.sqlContext.read
      .format("csv")
      .schema(QueryExecutionResult.infoResultsAsDataFrameSchema())
      .load(s"$hdfsRootDir/results/$filename")
      .select(columnNames.head, columnNames.tail: _*)
      .sort(columnNames(0))
      .foreach(row => println(row.mkString(",")))
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
