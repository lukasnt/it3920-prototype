package com.lukasnt.spark.io

import com.lukasnt.spark.io.CSVUtils.SnbCSVProperties
import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.models.{TemporalInterval, TemporalProperties}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import java.time.ZonedDateTime
import java.time.temporal.Temporal

class SNBLoader(val datasetRoot: String,
                propertiesLoader: TemporalPropertiesReader[ZonedDateTime],
                val fullGraph: Boolean = false)
    extends TemporalGraphLoader[ZonedDateTime] {

  private val DYNAMIC_DATA_GEN_ROOT = s"$datasetRoot/graphs/csv/raw/composite-merged-fk/dynamic"
  private val DYNAMIC_VERTEX_LABELS = List(
    "Person",
    "Comment",
    "Post"
    //"Forum"
  )
  private val DYNAMIC_EDGE_LABELS = List(
    ("Person_knows_Person", "Person1", "Person2"),
    //("Person_likes_Comment", "Person", "Comment"),
    //("Person_likes_Post", "Person", "Post"),
    ("Person_studyAt_University", "Person", "University"),
    ("Person_workAt_Company", "Person", "Company")
    //("Forum_hasMember_Person", "Forum", "Person")
    //("Comment_hasTag_Tag", "Comment", "Tag"),
    //("Forum_hasTag_Tag", "Forum", "Tag"),
    //("Person_hasInterest_Tag", "Person", "Tag"),
    //("Post_hasTag_Tag", "Post", "Tag")
  )

  override def load(sc: SparkContext): TemporalGraph = {
    // Load edges
    val edges: RDD[Edge[TemporalProperties[ZonedDateTime]]] = DYNAMIC_EDGE_LABELS
      .filter(label => fullGraph || label._1 == "Person_knows_Person")
      .map(label =>
        propertiesLoader.readEdgesFile(sc, s"$DYNAMIC_DATA_GEN_ROOT/${label._1}/", label._1, label._2, label._3))
      .reduce(_ union _)

    // Load vertices
    val vertices: RDD[(VertexId, TemporalProperties[ZonedDateTime])] = DYNAMIC_VERTEX_LABELS
      .filter(label => fullGraph || label == "Person")
      .map(label => propertiesLoader.readVerticesFile(sc, s"$DYNAMIC_DATA_GEN_ROOT/$label/", label))
      .reduce(_ union _)

    // Default vertex properties
    val defaultVertexProperties = new TemporalProperties[ZonedDateTime](
      findLifetimeInterval[ZonedDateTime](edges),
      "default",
      Map()
    )

    Graph(vertices, edges, defaultVertexProperties)
  }

  private def findLifetimeInterval[T <: Temporal](edges: RDD[Edge[TemporalProperties[T]]]): TemporalInterval[T] = {
    val minInterval: TemporalInterval[T] =
      edges
        .reduce((e1, e2) => {
          if (e1.attr.interval.before(e2.attr.interval)) e1 else e2
        })
        .attr
        .interval

    val maxInterval: TemporalInterval[T] = edges
      .reduce((e1, e2) => {
        if (e1.attr.interval.before(e2.attr.interval)) e2 else e1
      })
      .attr
      .interval

    new TemporalInterval[T](minInterval.startTime, maxInterval.endTime)
  }

}

object SNBLoader {

  def getByName(name: String,
                sqlContext: SQLContext,
                hdfsRootDir: String = "/user/root",
                fullGraph: Boolean = false): SNBLoader = name match {
    case "local-sf0_003" => localSf0_003
    case "sf0_003"       => sf0_003(sqlContext, hdfsRootDir, fullGraph)
    case "sf0_1"         => sf0_1(sqlContext, hdfsRootDir, fullGraph)
    case "sf0_3"         => sf0_3(sqlContext, hdfsRootDir, fullGraph)
    case "sf1"           => sf1(sqlContext, hdfsRootDir, fullGraph)
    case "sf3"           => sf3(sqlContext, hdfsRootDir, fullGraph)
    case "sf10"          => sf10(sqlContext, hdfsRootDir, fullGraph)
    case _               => sf0_003(sqlContext, hdfsRootDir, fullGraph)
  }

  def localSf0_003: SNBLoader = SNBLoader("/sf0_003-raw", PartitionedLocalCSV(SingleLocalCSV(SnbCSVProperties)))

  def sf0_003(sqlContext: SQLContext, hdfsRootDir: String, fullGraph: Boolean = false): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf0.003-raw", SparkCSV(sqlContext, SnbCSVProperties), fullGraph)

  def sf1(sqlContext: SQLContext, hdfsRootDir: String, fullGraph: Boolean = false): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf1-raw", SparkCSV(sqlContext, SnbCSVProperties), fullGraph)

  def sf0_1(sqlContext: SQLContext, hdfsRootDir: String, fullGraph: Boolean = false): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf0.1-raw", SparkCSV(sqlContext, SnbCSVProperties), fullGraph)

  def apply(datasetRoot: String,
            propertiesLoader: TemporalPropertiesReader[ZonedDateTime],
            fullGraph: Boolean = false): SNBLoader =
    new SNBLoader(datasetRoot, propertiesLoader, fullGraph)

  def sf0_3(sqlContext: SQLContext, hdfsRootDir: String, fullGraph: Boolean = false): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf0.3-raw", SparkCSV(sqlContext, SnbCSVProperties), fullGraph)

  def sf3(sqlContext: SQLContext, hdfsRootDir: String, fullGraph: Boolean = false): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf3-raw", SparkCSV(sqlContext, SnbCSVProperties), fullGraph)

  def sf10(sqlContext: SQLContext, hdfsRootDir: String, fullGraph: Boolean = false): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf10-raw", SparkCSV(sqlContext, SnbCSVProperties), fullGraph)
}
