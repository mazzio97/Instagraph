package com.instagraph.paths

import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable
import scala.reflect.ClassTag

// TODO: avoid the creation of a map as it is not an RDD
object ShortestPathsUtils {
  implicit class Manipulations[E: ClassTag](spGraph: Graph[Map[VertexId, ShortestPathInfo[E]], E]) {
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
              recursion(spMap(nextId)(destination).successors.keySet)
              currentPath.pop()
            })
          }
        }
        recursion(info.successors.keySet)
        (destination, (info.totalCost, paths.toSet))
      }
    }

    def shortestPathsFrom(origin: VertexId): Graph[(E, Set[List[VertexId]]), E] = {
      val ssspMap = shortestPathsMapFrom(origin)
      spGraph.mapVertices { case(id, _) => ssspMap(id) }
    }
  }
}
