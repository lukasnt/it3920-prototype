package com.lukasnt.spark.io

import com.lukasnt.spark.io.InteractionLoader.getIdFieldAsLong
import com.lukasnt.spark.models.Types.{Properties, TemporalGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexRDD

import java.time.ZonedDateTime

class InteractionLoader(val snbLoader: TemporalGraphLoader[ZonedDateTime]) extends TemporalGraphLoader[ZonedDateTime] {

  override def load(sc: SparkContext): TemporalGraph = {
    val rawGraph: TemporalGraph = snbLoader.load(sc)

    val comments: VertexRDD[Properties] = rawGraph.vertices.filter {
      case (_, attr) => attr.typeLabel == "Comment"
    }
    val posts: VertexRDD[Properties] = rawGraph.vertices.filter {
      case (_, attr) => attr.typeLabel == "Post"
    }

    val parentCommentCreators = comments.map {
      case (_, attr) => (getIdFieldAsLong(attr, "ParentCommentId"), getIdFieldAsLong(attr, "CreatorPersonId"))
    }
    val parentPostCreators = comments.map {
      case (_, attr) => (getIdFieldAsLong(attr, "ParentPostId"), getIdFieldAsLong(attr, "CreatorPersonId"))
    }

    val personReplyToPersonComment = parentCommentCreators
      .join(comments)
      .map {
        case (parentCommentId, (replyCommentCreatorId, parentCommentAttr)) =>
          (replyCommentCreatorId, getIdFieldAsLong(parentCommentAttr, "CreatorPersonId"))
      }
      .groupBy {
        case (replyCommentCreatorId, parentCommentCreatorId) => (replyCommentCreatorId, parentCommentCreatorId)
      }
      .mapValues(_.size)
      .collect()
      .toMap

    val personReplyToPersonPost = parentPostCreators
      .join(posts)
      .map {
        case (parentPostId, (replyCommentCreatorId, parentPostAttr)) =>
          (replyCommentCreatorId, getIdFieldAsLong(parentPostAttr, "CreatorPersonId"))
      }
      .groupBy {
        case (replyCommentCreatorId, parentPostCreatorId) => (replyCommentCreatorId, parentPostCreatorId)
      }
      .mapValues(_.size)
      .collect()
      .toMap

    val result: TemporalGraph = rawGraph
      .subgraph(epred = triplet => triplet.attr.typeLabel == "Person_knows_Person",
                vpred = (_, attr) => attr.typeLabel == "Person")
      .mapTriplets(triplet => {
        val numInteractions: Int = personReplyToPersonComment.getOrElse((triplet.srcId, triplet.dstId), 0) +
          personReplyToPersonComment.getOrElse((triplet.dstId, triplet.srcId), 0) +
          personReplyToPersonPost.getOrElse((triplet.srcId, triplet.dstId), 0) +
          personReplyToPersonPost.getOrElse((triplet.dstId, triplet.srcId), 0)
        new Properties(triplet.attr.interval,
                       triplet.attr.typeLabel,
                       triplet.attr.properties + ("numInteractions" -> numInteractions.toString))
      })

    result
  }

}

object InteractionLoader {

  private def getIdFieldAsLong(properties: Properties, idField: String): Long = {
    if (properties.properties(idField).nonEmpty) properties.properties(idField).toLong else -1
  }

}
