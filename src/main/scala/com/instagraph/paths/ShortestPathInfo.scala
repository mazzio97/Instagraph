package com.instagraph.paths

import org.apache.spark.graphx.VertexId

/**
 * Data structure to represent info about a shortest path.
 *
 * @tparam E the edge type
 */
trait ShortestPathInfo[+E] {
  def nextVertex: Option[VertexId]
  def totalCost: E
}

/**
 * Implementation of the ShortestPathInfo trait
 */
object ShortestPathInfo {
  def apply[E](nextVertex: VertexId, totalCost: E): ShortestPathInfo[E] = ShortestPathInfoImpl(Option(nextVertex), totalCost)

  def apply[E](totalCost: E): ShortestPathInfo[E] = ShortestPathInfoImpl(Option.empty, totalCost)

  def toSelf[E](implicit numeric: Numeric[E]): ShortestPathInfo[E] = ShortestPathInfo(numeric.zero)

  private case class ShortestPathInfoImpl[E](nextVertex: Option[VertexId], totalCost: E) extends ShortestPathInfo[E]
}