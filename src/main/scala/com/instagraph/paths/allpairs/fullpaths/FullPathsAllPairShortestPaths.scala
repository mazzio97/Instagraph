package com.instagraph.paths.allpairs.fullpaths

import com.instagraph.paths.{FullPathsInfo, ShortestPathsWithAdjacentVerticesInfo}
import com.instagraph.utils.MapUtils._
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Abstract class that computes the shortest paths from each vertex to each other vertex
 *
 * The results are stored into a map where the key is the origin vertex and the value is a graph in which each vertex
 * contains the full shortest path(s) from the key to the vertex in that specific graph
 *
 * The graphs obtained by each key of the map are computed from a previous computation of the all-pair-shortest-paths
 * graph (whose computation will be performed as soon as the class is instantiated), and in particular their computation
 * is performed whenever necessary, hence be sure not to call the computation more times as it is very expensive (also
 * notice that filtering, mapping and grouping will trigger the computation, that is why is highly recommended not to
 * use those methods on this map)
 *
 * @tparam V the vertex type
 * @tparam E the edge type
 * @tparam I the info type
 * @tparam S the shortest path info type
 */
abstract class FullPathsAllPairShortestPaths[
  V: ClassTag,
  E: ClassTag,
  I <: FullPathsInfo[E] : ClassTag,
  S <: ShortestPathsWithAdjacentVerticesInfo[E] : ClassTag
](protected val spGraph: Graph[Map[VertexId, S], E])(implicit numeric: Numeric[E]) extends Serializable {
  type HeadsMap = Map[VertexId, Set[VertexId]]

  final val vertices: Set[VertexId] = spGraph.vertices.keys.toLocalIterator.toSet

  protected def initializeInfo(cost: E, paths: List[VertexId]*): I

  /**
   * Throws an IllegalArgumentException if the vertex is not included in the set of vertices
   *
   * @param origin the origin vertex
   * @return a graph containing the shortest paths from the origin to each other vertex
   */
  final def apply(origin: VertexId): Graph[I, E] =
    if (vertices.contains(origin)) from(origin)
    else throw new IllegalArgumentException(s"$origin is not one of the vertices")

  final def iterator: Iterator[(VertexId, Graph[I, E])] =
    spGraph.vertices.keys.toLocalIterator.map(origin => (origin, from(origin)))

  final def foreach[U](f: (VertexId, Graph[I, E]) => U) : Unit =
    iterator.foreach { case(origin, fpGraph) => f(origin, fpGraph) }

  private def from(origin: VertexId): Graph[I, E] = {
    /*
     * the initial graph is a subgraph of the shortest path graph containing just the vertices that have at least one
     * path from the given origin to them and just the edges that are traversed during those shortest paths, then each
     * vertex is mapped to a list containing the vertex itself (namely the destination) and its predecessor when present
     *
     * also, at the beginning the heads map (namely the map associating to each destination the current heads of the
     * shortest paths it has) has one record only, that one for the destination itself, and it contains the predecessors
     * of the destination vertex
     */
    val initialGraph: Graph[(I, HeadsMap), E] = spGraph.mapVertices((_, spMap) => spMap.get(origin))
      .subgraph(vpred = (_, info) => info.isDefined)
      .mapVertices((_, info) => info.get)
      .subgraph(epred = triplet => triplet.dstAttr.adjacentVertices.contains(triplet.srcId))
      .mapVertices((destination, info) => {
        val paths: Set[List[VertexId]] =
          if (info.adjacentVertices.isEmpty) Set(List(destination))
          else info.adjacentVertices.map(List(_, destination))
        (initializeInfo(info.totalCost, paths.toList: _*), Map(destination -> info.adjacentVertices))
      })

    /*
     * during the first iteration, when the vertex receives the empty map, the status does not change
     * from that point onwards, to each path is prepended the respective head received during the last iteration
     * (if the path starts from the origin already, there is no need to do anything, thus the path is packed into a
     *  sequence in order to be flat mapped with the other ones) and, finally, the new heads map replaces the old one
     */
    val vertexProgram: (VertexId, (I, HeadsMap), HeadsMap) => (I, HeadsMap) =
      (_, status, receivedHeadsMap) => {
        if (receivedHeadsMap.isEmpty) status
        else {
          val info: I = status._1
          val newPaths: Set[List[VertexId]] = info.paths.flatMap { path =>
            val currentHead: VertexId = path.head
            if (currentHead == origin) Seq(path)
            else receivedHeadsMap(currentHead).map(newHead => List(newHead) ++ path)
          }
          (initializeInfo(info.cost, newPaths.toList: _*), receivedHeadsMap)
        }
      }

    /*
     * the heads map is propagated from each vertex to their successors until each destination
     * by doing so, during the first iteration a vertex will say to its successors which vertices are its predecessors,
     * then at the next iteration it will say which vertices are the predecessors of their predecessors, and so on,
     * so that at the end every vertices will be able to get the predecessors until the origin
     */
    val sendMessage: EdgeTriplet[(I, HeadsMap), E] => Iterator[(VertexId, HeadsMap)] =
      triplet => {
        val successor: VertexId = triplet.dstId
        val headsMap: HeadsMap = triplet.srcAttr._2
        if (headsMap.isEmpty) Iterator.empty else Iterator((successor, headsMap))
      }

    val mergeMessages: (HeadsMap, HeadsMap) => HeadsMap =
      (v, w) => v.merge(w, (_, vHeads, wHeads) => vHeads ++ wHeads)

    initialGraph.pregel[HeadsMap](initialMsg = Map.empty, activeDirection = EdgeDirection.Both)(
      vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
    ).mapVertices((_, tuple) => tuple._1)
  }
}