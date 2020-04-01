package com.instagraph.paths.allpairs

import com.instagraph.paths.ShortestPathsInfo
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class CostOnly[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E],
  override protected val direction: PathsDirection
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, CostOnlyInfo[E]] {
  type Info = CostOnlyInfo[E]

  override protected def infoAbout(adjacentId: Option[VertexId], cost: E, adjacent: Option[Info]): Info =
    CostOnlyInfo(cost)

  // having no adjacent vertices, we have no clues whether we are sending the same info or not
  // so we ignore this check and pass the control to the abstract function which will check the cost
  override protected def sendingSameInfo(adjacentId: VertexId, updatedAdjacentInfo: Info, firstInfo: Info): Boolean =
    false

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option.empty

}

case class CostOnlyInfo[C](override val totalCost: C) extends ShortestPathsInfo[C]
