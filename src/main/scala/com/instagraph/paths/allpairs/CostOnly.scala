package com.instagraph.paths.allpairs

import com.instagraph.paths.allpairs.adjacents.AllPairShortestPaths
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

case class CostOnly[V: ClassTag, E: ClassTag](
  override val graph: Graph[V, E]
)(implicit numeric: Numeric[E]) extends AllPairShortestPaths[V, E, CostOnlyInfo[E]] {
  type Info = CostOnlyInfo[E]

  override protected val backwardPath: Boolean = false

  override protected def infoAbout(adjacentId: Option[VertexId], cost: E, adjacent: Option[Info]): Info =
    CostOnlyInfo(cost)

  // having no adjacent vertices, we have no clues whether we are sending the same info or not
  // so we ignore this check and pass the control to the abstract function which will check the cost
  override protected def sendingSameInfo(adjacentId: VertexId, updatedAdjacentInfo: Info, firstInfo: Info): Boolean =
    false

  override protected def mergeSameCost(mInfo: Info, nInfo: Info): Option[Info] =
    Option.empty
}
