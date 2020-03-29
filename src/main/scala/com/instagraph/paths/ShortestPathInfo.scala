package com.instagraph.paths

import com.instagraph.paths.ShortestPathInfo.SuccessorsMap
import org.apache.spark.graphx.VertexId

/**
 * Implementation of the ShortestPathInfo trait
 */
object ShortestPathInfo {
  type SuccessorsMap = Map[VertexId, Int]

  def apply[E](nextVertex: VertexId, presences: Int, totalCost: E): ShortestPathInfo[E] =
    ShortestPathInfoImpl(Map(nextVertex -> presences), totalCost)

  def toSelf[E](implicit numeric: Numeric[E]): ShortestPathInfo[E] =
    ShortestPathInfoImpl(Map.empty, numeric.zero)

  private case class ShortestPathInfoImpl[E](override val successors: SuccessorsMap, override val totalCost: E)
    extends ShortestPathInfo[E] {
    override def addSuccessors(s: (VertexId, Int)*): ShortestPathInfo[E] =
      ShortestPathInfoImpl(successors ++ s.toMap, totalCost)
  }
}

/**
 * Data structure to represent info about a shortest path.
 *
 * @tparam E the edge type
 */
trait ShortestPathInfo[+E] {
  val successors: SuccessorsMap
  val totalCost: E
  def addSuccessors(s: (VertexId, Int)*): ShortestPathInfo[E]
}