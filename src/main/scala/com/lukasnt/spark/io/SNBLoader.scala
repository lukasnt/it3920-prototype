package com.lukasnt.spark.io

import com.lukasnt.spark.models.Types.TemporalGraph
import com.lukasnt.spark.models.{TemporalInterval, TemporalProperties}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime
import java.time.temporal.Temporal
import scala.collection.mutable

class SNBLoader(val folderPath: String,
                propertiesLoader: TemporalPropertiesLoader[ZonedDateTime],
                val format: String = "csv")
    extends TemporalGraphLoader[ZonedDateTime] {

  private val DATA_GEN_ROOT = s"$folderPath/dynamic"
  private val VERTEX_LABELS = List(
    "Person",
    "Forum",
    "Comment"
  )
  private val EDGE_LABELS = List(
    ("Comment_hasTag_Tag", "Comment", "Tag"),
    ("Forum_hasMember_Person", "Forum", "Person"),
    ("Forum_hasTag_Tag", "Forum", "Tag"),
    ("Person_hasInterest_Tag", "Person", "Tag"),
    ("Person_knows_Person", "Person1", "Person2"),
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

    // Load edges
    val edgeLabelFiles = createLabelFilesMap(EDGE_LABELS.map(_._1))
    for (label <- EDGE_LABELS) {
      val labelName = label._1
      val srcLabel  = label._2
      val dstLabel  = label._3
      val edgeFiles = edgeLabelFiles(labelName)
      for (file <- edgeFiles) {
        println(s"label: ${label.toString}")
        val edgesFile = propertiesLoader.readEdgesFile(sc, file, labelName, srcLabel, dstLabel)
        edges = edges.union(edgesFile)
      }
    }

    // Load vertices
    val vertexLabelFiles = createLabelFilesMap(VERTEX_LABELS)
    for (label <- VERTEX_LABELS) {
      val vertexFiles = vertexLabelFiles(label)
      for (file <- vertexFiles) {
        println(s"label: $label")
        val verticesFile = propertiesLoader.readVerticesFile(sc, file, label)
        vertices = vertices.union(verticesFile)
      }
    }

    // Default vertex properties
    val defaultVertexProperties = new TemporalProperties[ZonedDateTime](
      findLifetimeInterval[ZonedDateTime](edges),
      "default",
      Map()
    )

    Graph(vertices, edges, defaultVertexProperties)
  }

  private def createLabelFilesMap(labels: List[String]): Map[String, List[String]] = {
    val labelFilesMap: mutable.Map[String, List[String]] = mutable.Map[String, List[String]]()
    for (label <- labels) {
      val dirPath = s"$DATA_GEN_ROOT/$label/"
      val fileNames = DirUtils
        .listFilesInsideJar(dirPath)
        .map(_.substring(dirPath.length))
        .filter(name => name.startsWith("part-") && name.endsWith(s".$format"))
      labelFilesMap += (label -> fileNames.map(dirPath + _))
    }
    labelFilesMap.toMap
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
