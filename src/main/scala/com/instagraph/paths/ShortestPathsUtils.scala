package com.instagraph.paths

import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable
import scala.reflect.ClassTag

// TODO: avoid the creation of a map as it is not an RDD
object ShortestPathsUtils {
  implicit class Manipulations[E: ClassTag, I <: ShortestPathsWithAdjacentVerticesInfo[E]](spGraph: Graph[Map[VertexId, I], E]) {
    private def shortestPathsMapFrom(origin: VertexId): Map[VertexId, (E, Set[List[VertexId]])] = {
      val spMap = spGraph.vertices.collectAsMap().toMap
      spMap(origin).map { case(destination, info) =>
        val paths: mutable.Set[List[VertexId]] = mutable.Set.empty
        val currentPath: mutable.Stack[VertexId] = mutable.Stack(origin)
        def recursion(successors: Set[VertexId]): Unit = {
          if (successors.isEmpty) {
            paths.add(currentPath.toList.reverse)
          } else {
            successors.foreach(nextId => {
              currentPath.push(nextId)
              recursion(spMap(nextId)(destination).adjacentVertices)
              currentPath.pop()
            })
          }
        }
        recursion(info.adjacentVertices)
        (destination, (info.totalCost, paths.toSet))
      }
    }

    def shortestPathsFrom(origin: VertexId): Graph[(E, Set[List[VertexId]]), E] = {
      val ssspMap = shortestPathsMapFrom(origin)
      spGraph.mapVertices { case(id, _) => ssspMap(id) }
    }
  }
}
