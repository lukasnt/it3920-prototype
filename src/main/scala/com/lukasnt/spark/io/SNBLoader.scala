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

class SNBLoader(val datasetRoot: String, propertiesLoader: TemporalPropertiesReader[ZonedDateTime])
    extends TemporalGraphLoader[ZonedDateTime] {

  private val DATA_GEN_ROOT = s"$datasetRoot/graphs/csv/raw/composite-merged-fk/dynamic"
  private val VERTEX_LABELS = List(
    "Person"
    //"Forum",
    //"Comment"
  )
  private val EDGE_LABELS = List(
    /*("Comment_hasTag_Tag", "Comment", "Tag"),
    ("Forum_hasMember_Person", "Forum", "Person"),
    ("Forum_hasTag_Tag", "Forum", "Tag"),
    ("Person_hasInterest_Tag", "Person", "Tag"),*/
    ("Person_knows_Person", "Person1", "Person2")
    /*("Person_likes_Comment", "Person", "Comment"),
    ("Person_likes_Post", "Person", "Post"),
    ("Person_studyAt_University", "Person", "University"),
    ("Person_workAt_Company", "Person", "Company"),
    ("Post_hasTag_Tag", "Post", "Tag")*/
  )

  override def load(sc: SparkContext): TemporalGraph = {
    // Load edges
    val edges: RDD[Edge[TemporalProperties[ZonedDateTime]]] = EDGE_LABELS
      .map(label => propertiesLoader.readEdgesFile(sc, s"$DATA_GEN_ROOT/${label._1}/", label._1, label._2, label._3))
      .reduce(_ union _)

    // Load vertices
    val vertices: RDD[(VertexId, TemporalProperties[ZonedDateTime])] = VERTEX_LABELS
      .map(label => propertiesLoader.readVerticesFile(sc, s"$DATA_GEN_ROOT/$label/", label))
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

  def localSf0_003: SNBLoader = SNBLoader("/sf0_003-raw", PartitionedLocalCSV(SingleLocalCSV(SnbCSVProperties)))

  def apply(datasetRoot: String, propertiesLoader: TemporalPropertiesReader[ZonedDateTime]): SNBLoader =
    new SNBLoader(datasetRoot, propertiesLoader)

  def sf0_003(sqlContext: SQLContext, hdfsRootDir: String): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf0.003-raw", SparkCSV(sqlContext, SnbCSVProperties))

  def sf1(sqlContext: SQLContext, hdfsRootDir: String): SNBLoader =
    SNBLoader(s"$hdfsRootDir/sf1-raw", SparkCSV(sqlContext, SnbCSVProperties))

}
