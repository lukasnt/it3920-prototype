package com.lukasnt.spark

import com.lukasnt.spark.examples.SimpleParameterQueries
import com.lukasnt.spark.executors.SparkQueryExecutor
import com.lukasnt.spark.experiments.{ArrayIntPairConverter, Experiment, IntPairConverter, VariableOrderConverter, VariableSet}
import com.lukasnt.spark.io.SNBLoader
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
      @Option(names = Array("-n", "--name"), description = Array("Experiment name"), defaultValue = "Experiment")
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
      @Option(
        names = Array("-lrv", "--length-range-variables"),
        description = Array("Length range variables"),
        converter = Array(classOf[IntPairConverter]),
        defaultValue = "1,2"
      )
      lengthRangeVariables: Array[(Int, Int)] = Array((1, 2), (3, 4)),
      @Option(names = Array("-kv", "--top-k-variables"),
              description = Array("number of top k results"),
              defaultValue = "3")
      topKVariables: Array[Int] = Array(3)
  ): Unit = {

    println(
      s"Experiment: $name, " +
        s"maxVariables: $maxVariables, " +
        s"runsPerVariable: $runsPerVariable, " +
        s"variableOrder: $variableOrder, " +
        s"lengthRangeVariables: ${lengthRangeVariables.mkString(", ")}, " +
        s"topKVariables: ${topKVariables.mkString(", ")}"
    )

    val spark = SparkSession.builder().getOrCreate()
    Experiment
      .builder()
      .withName(name)
      .withSparkSession(spark)
      .withMaxVariables(maxVariables)
      .withRunsPerVariable(runsPerVariable)
      .withVariableOrder(Experiment.VariableOrder.Ascending)
      .withVariableSet(
        VariableSet
          .builder()
          .fromParameterQuery(SimpleParameterQueries.interactionPaths())
          .withTopKVariables(topKVariables.toList)
          .withLengthRangeVariables(lengthRangeVariables.toList)
          .withExecutorVariables(List(SparkQueryExecutor()))
          .withGraphLoaderVariables(List(SNBLoader.sf1(spark.sqlContext, hdfsRootDir)))
          .build())
      .build()
      .run()
  }

  override def call(): Int = {
    0
  }

}

object App {

  def main(args: Array[String]): Unit = {
    println("Hello World!")
    System.exit(new CommandLine(new App()).execute(args: _*))
  }

}
