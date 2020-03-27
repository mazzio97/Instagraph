package com.instagraph.paths

import org.apache.spark.graphx.VertexId

/**
 * Data structure to represent info about a shortest path.
 *
 * @tparam E the edge type
 */
trait ShortestPathInfo[+E] {
  val successors: Set[VertexId]
  val totalCost: E
  def addSuccessors(id: VertexId*): ShortestPathInfo[E]
}

/**
 * Implementation of the ShortestPathInfo trait
 */
object ShortestPathInfo {
  def apply[E](nextVertex: VertexId, totalCost: E): ShortestPathInfo[E] = ShortestPathInfoImpl(Set(nextVertex), totalCost)

  def apply[E](totalCost: E): ShortestPathInfo[E] = ShortestPathInfoImpl(Set.empty, totalCost)

  def toSelf[E](implicit numeric: Numeric[E]): ShortestPathInfo[E] = ShortestPathInfo(numeric.zero)

  private case class ShortestPathInfoImpl[E](
    override val successors: Set[VertexId],
    override val totalCost: E
  ) extends ShortestPathInfo[E] {
    override def addSuccessors(id: VertexId*): ShortestPathInfo[E] = ShortestPathInfoImpl(successors ++ id.toSet, totalCost)
  }
}