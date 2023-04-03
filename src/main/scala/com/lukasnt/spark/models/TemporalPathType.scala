package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.Interval

import java.time.temporal.ChronoUnit

/**
  * This trait defines the temporal path type based on valid interval relation between consecutive edges
  * and a nextInterval function used to extend the temporal path.
  * Implementations could be ContinuousPath, PairwiseContinuous, ConsecutivePath etc.
  */
trait TemporalPathType extends Serializable {

  /**
    * This function should return true if the edge interval is valid based on the last interval of the path according to the temporal path type.
    * @param lastInterval last interval of the path
    * @param edgeInterval interval of the edge
    * @return true if the edge interval is valid based on the last interval of the path according to the temporal path type
    */
  def validEdgeInterval(lastInterval: Interval, edgeInterval: Interval): Boolean

  /**
    * Based on the last interval of the path and the edge interval, this function should return the next valid interval of the path.
    * The next interval should satisfy the following constraints:
    * <list>
    *   <li> nextInterval needs to be a sub-interval of edgeInterval  </li>
    *   <li> if (!validInterval(lastInterval, edgeInterval)) nextInterval = null </li>
    * </list>
    * The next interval does not have any further constraints.
    * However, it should be as large as possible while capturing the semantics of the temporal path type,
    * such that all valid paths of the temporal path type can be found.
    * @param lastInterval last interval of the path
    * @param edgeInterval interval of the edge
    * @return next interval of the path
    */
  def nextInterval(lastInterval: Interval, edgeInterval: Interval): Interval

  /**
    * This function should return the initial interval of the path given the next first edge.
    * The initial interval have to satisfy the following:
    * <list>
    *   <li> validEdgeInterval(initInterval, edgeInterval) == true </li>
    *   <li> nextInterval(initInterval, edgeInterval) will be the first interval of the path </li>
    * </list>
    * @param edgeInterval interval of the edge first edge of the path
    * @return initial interval of the path with the given start node and the next first edge
    */
  def initInterval(edgeInterval: Interval): Interval

}

object TemporalPathType {

  def Continuous: TemporalPathType = new TemporalPathType {
    override def validEdgeInterval(lastInterval: Interval, edgeInterval: Interval): Boolean = {
      lastInterval.overlaps(edgeInterval)
    }

    override def nextInterval(lastInterval: Interval, edgeInterval: Interval): Interval = {
      if (validEdgeInterval(lastInterval, edgeInterval))
        lastInterval.intersection(edgeInterval)
      else TemporalInterval()
    }

    override def initInterval(edgeInterval: Interval): Interval = {
      edgeInterval
    }
  }

  def PairwiseContinuous: TemporalPathType = new TemporalPathType {
    override def validEdgeInterval(lastInterval: Interval, edgeInterval: Interval): Boolean = {
      lastInterval.overlaps(edgeInterval)
    }

    override def nextInterval(lastInterval: Interval, edgeInterval: Interval): Interval = {
      if (validEdgeInterval(lastInterval, edgeInterval))
        edgeInterval
      else TemporalInterval()
    }

    override def initInterval(edgeInterval: Interval): Interval = {
      edgeInterval
    }
  }

  def Consecutive: TemporalPathType = new TemporalPathType {
    override def validEdgeInterval(lastInterval: Interval, edgeInterval: Interval): Boolean = {
      lastInterval.before(edgeInterval)
    }

    override def nextInterval(lastInterval: Interval, edgeInterval: Interval): Interval = {
      if (validEdgeInterval(lastInterval, edgeInterval))
        new Interval(edgeInterval.endTime, edgeInterval.endTime)
      else TemporalInterval()
    }

    override def initInterval(edgeInterval: Interval): Interval = {
      new Interval(edgeInterval.startTime.minus(1, ChronoUnit.NANOS), edgeInterval.startTime)
    }
  }

  def NonTemporal: TemporalPathType = new TemporalPathType {
    override def validEdgeInterval(lastInterval: Interval, edgeInterval: Interval): Boolean = {
      true
    }

    override def nextInterval(lastInterval: Interval, edgeInterval: Interval): Interval = {
      edgeInterval
    }

    override def initInterval(edgeInterval: Interval): Interval = {
      edgeInterval
    }
  }

}
