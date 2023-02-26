package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.models.{TemporalInterval, TemporalProperties}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime
import java.time.temporal.Temporal

class SNBLoader(val folderPath: String, propertiesLoader: TemporalPropertiesLoader[ZonedDateTime])
    extends TemporalGraphLoader[ZonedDateTime] {

  private val DATA_GEN_ROOT = s"$folderPath/dynamic"
  private val VERTEX_LABELS = List(
    "Person",
    "Tag",
    "Forum",
    "Comment"
  )
  private val EDGE_LABELS = List(
    ("Comment_hasTag_Tag", "Comment", "Tag"),
    ("Forum_hasMember_Person", "Forum", "Person"),
    ("Forum_hasTag_Tag", "Forum", "Tag"),
    ("Person_hasInterest_Tag", "Person", "Tag"),
    ("Person_knows_Person", "Person", "Person"),
    ("Person_likes_Comment", "Person", "Comment"),
    ("Person_likes_Post", "Person", "Post"),
    ("Person_studyAt_University", "Person", "University"),
    ("Person_workAt_Company", "Person", "Company"),
    ("Post_hasTag_Tag", "Post", "Tag")
  )

  override def load(sc: SparkContext): TemporalGraph[ZonedDateTime] = {

    // Initialize the RDD of vertices and edges
    var vertices: RDD[(VertexId, TemporalProperties[ZonedDateTime])] = sc.emptyRDD
    var edges: RDD[Edge[TemporalProperties[ZonedDateTime]]]          = sc.emptyRDD

    for ((label, partitionedFiles) <- createLabelFilesMap()) {
      // Read the file and add the edges to the RDD
      edges = partitionedFiles
        .map(
          file =>
            propertiesLoader
              .readEdgesFile(sc, file, label)
        )
        .reduce(_ union _) union edges
    }

    // Default vertex properties
    val defaultVertexProperties = new TemporalProperties[ZonedDateTime](
      findLifetimeInterval[ZonedDateTime](edges),
      "default",
      Map()
    )

    Graph.fromEdges(edges, defaultVertexProperties)
  }

  private def createLabelFilesMap(): Map[String, Array[String]] = {
    // Find all subdirectories which are named after the label
    val subDirs = new java.io.File(DATA_GEN_ROOT).listFiles
      .filter(_.isDirectory)
      .filter(f => /*VERTEX_LABELS.contains(f) ||*/ EDGE_LABELS.map(_._1).contains(f.getName))

    // Find all files that start with "part" and end with ".csv"
    val files = subDirs.map(
      _.listFiles
        .filter(f => f.getName.startsWith("part") && f.getName.endsWith(".csv"))
        .map(_.getName))

    // Create a map of label -> files
    subDirs.map(_.getName).zip(files).toMap
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
