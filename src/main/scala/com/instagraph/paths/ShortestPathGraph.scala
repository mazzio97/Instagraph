package com.instagraph.paths

import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object ShortestPathGraph {
  implicit class Manipulations[E: ClassTag](val spGraph: Graph[Map[VertexId, ShortestPathInfo[E]], E]) {
    def toShortestPathMap: Map[VertexId, Map[VertexId, ShortestPathInfo[E]]] =
      spGraph.vertices.collectAsMap().toMap

    def shortestPathsMapFrom(origin: VertexId): Map[VertexId, (E, List[VertexId])] = {
      val spMap = toShortestPathMap

      spMap(origin).map { case(destination, info) =>
        val path: List[VertexId] = if (info.nextVertex.isEmpty) List(origin) else {
          val list: ListBuffer[VertexId] = ListBuffer(origin)
          @scala.annotation.tailrec
          def tailRecursion(node: VertexId): Unit = {
            list += node
            val next: Option[VertexId] = spMap(node)(destination).nextVertex
            if (next.isDefined) tailRecursion(next.get)
          }
          tailRecursion(info.nextVertex.get)
          list.toList
        }
        (destination, (info.totalCost, path))
      }
    }

    def shortestPathGraphFrom(origin: VertexId): Graph[(E, List[VertexId]), E] = {
      val ssspMap = shortestPathsMapFrom(origin)
      spGraph.mapVertices { case(id, _) => ssspMap(id) }
    }
  }
}
