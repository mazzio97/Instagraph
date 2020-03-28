package com.instagraph.paths

import org.apache.spark.graphx.VertexId

/**
 * Data structure to represent info about a shortest path.
 *
 * @tparam E the edge type
 */
trait ShortestPathInfo[+E] {
  val successors: Map[VertexId, Int]
  val totalCost: E
  def addSuccessors(s: (VertexId, Int)*): ShortestPathInfo[E]
}

/**
 * Implementation of the ShortestPathInfo trait
 */
object ShortestPathInfo {
  def apply[E](nextVertex: VertexId, presences: Int, totalCost: E): ShortestPathInfo[E] =
    ShortestPathInfoImpl(Map(nextVertex -> presences), totalCost)

  def toSelf[E](implicit numeric: Numeric[E]): ShortestPathInfo[E] =
    ShortestPathInfoImpl(Map.empty, numeric.zero)

  private case class ShortestPathInfoImpl[E](override val successors: Map[VertexId, Int], override val totalCost: E)
    extends ShortestPathInfo[E] {
    override def addSuccessors(s: (VertexId, Int)*): ShortestPathInfo[E] =
      ShortestPathInfoImpl(successors ++ s.toMap, totalCost)
  }
}