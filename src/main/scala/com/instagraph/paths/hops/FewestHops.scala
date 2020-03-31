package com.instagraph.paths.hops

import org.apache.spark.graphx.{Graph, VertexId, lib}
import org.apache.spark.graphx.lib.ShortestPaths.SPMap

import scala.reflect.ClassTag

/**
 * Computes fewest hops on a graph
 *
 * @param graph the input graph
 * @tparam V the vertex type
 * @tparam E the edge type
 */
class FewestHops[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
  def fewestHops(landmarks: Seq[VertexId]): Graph[SPMap, E] = {
    lib.ShortestPaths.run(graph, landmarks)
  }
}