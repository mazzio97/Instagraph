package com.instagraph.utils

import com.instagraph.indices.centrality.CentralityIndex
import org.apache.spark.graphx.{Edge, Graph}

import scala.reflect.ClassTag

object GraphUtils {
  implicit class Manipulations[V: ClassTag, E: ClassTag](graph: Graph[V, E]) {
    def without(vertices: V*): Graph[V, E] = graph.subgraph(e =>
      !vertices.contains(e.srcAttr) && !vertices.contains(e.dstAttr),
      (_, user) => !vertices.contains(user)
    )

    def mergeWith(otherGraph: Graph[V, E]): Graph[V, E] = {
      val vertices = graph.vertices.map(_._2).union(otherGraph.vertices.map(_._2)).distinct.zipWithIndex
      def updateEdges(g: Graph[V, E]) =
        g.triplets
          .map(et => (et.srcAttr, (et.attr, et.dstAttr)))
          .join(vertices)
          .map { case (oldIdSrc, ((edge, dstId), newIdSrc)) => (dstId, (newIdSrc, edge)) }
          .join(vertices)
          .map { case (oldIdDst, ((newIdSrc, edge), newIdDst)) => new Edge(newIdSrc, newIdDst, edge) }
      Graph(
        vertices.map(_.swap),
        updateEdges(graph) union updateEdges(otherGraph)
      )
    }

    def onlyBidirectional: Graph[V, E] = {
      graph.subgraph(epred = e1 =>
        graph.edges.filter(e2 => e1 == Edge(e2.dstId, e2.srcId, e2.attr)).count() > 0
      )
    }

    def transformWith[M: ClassTag](index: CentralityIndex[M]): Graph[(V, M), E] = Graph(
      index.compute(graph)
        .vertices
        .join(graph.vertices)
        .mapValues(_.swap),
      graph.edges
    )
  }
}
