package com.lukasnt.spark.io

import com.lukasnt.spark.io.RecruitmentLoader.getIdFieldAsLong
import com.lukasnt.spark.models.Types.{Properties, TemporalGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, PartitionStrategy}

import java.time.ZonedDateTime

class RecruitmentLoader(val snbLoader: TemporalGraphLoader[ZonedDateTime]) extends TemporalGraphLoader[ZonedDateTime] {

  override def load(sc: SparkContext): TemporalGraph = {
    val rawGraph: TemporalGraph = snbLoader.load(sc).partitionBy(PartitionStrategy.RandomVertexCut)

    val personStudyAtUniversity = rawGraph.edges
      .filter(_.attr.typeLabel == "Person_studyAt_University")
      .groupBy {
        case Edge(_, _, attr) => getIdFieldAsLong(attr, "PersonId")
      }

    val personWorkAtCompany = rawGraph.edges
      .filter(_.attr.typeLabel == "Person_workAt_Company")
      .groupBy {
        case Edge(_, _, attr) => getIdFieldAsLong(attr, "PersonId")
      }

    val mappedVertices = rawGraph.vertices
      .filter {
        case (_, attr) => attr.typeLabel == "Person"
      }
      .map {
        case (id, attr) => (id, attr.typeLabel)
      }

    ???

  }

}

object RecruitmentLoader {
  private def getIdFieldAsLong(properties: Properties, idField: String): Long = {
    if (properties.properties(idField).nonEmpty) properties.properties(idField).toLong else -1
  }
}
