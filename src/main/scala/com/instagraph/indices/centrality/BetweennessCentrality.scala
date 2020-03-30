package com.instagraph.indices.centrality

import com.instagraph.paths.ShortestPaths.Distances
import com.instagraph.utils.MapUtils.Manipulations
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

case class BetweennessCentrality[E: ClassTag](override val numeric: Numeric[E]) extends NumericCentralityIndex[Double, E] {
  type NormalizedSuccessorsMap = Map[VertexId, Double]
  type ShortestPathsMap = Map[VertexId, NormalizedSuccessorsMap]
  type ScoresMap = Map[VertexId, Double]

  private case class VertexData(score: Double, shortestPaths: ShortestPathsMap, latestReceivedScores: ScoresMap)

  override def compute[V: ClassTag](graph: Graph[V, E]): Graph[Double, E] = {
    val initialGraph: Graph[VertexData, E] = graph.allPairsShortestPath(numeric).mapVertices { (_, shortestPaths) =>
      val updatedShortestPaths: ShortestPathsMap = shortestPaths.map { case (destination, info) =>
        val totalShortestPaths: Int = info.successors.values.sum
        val successorsMap: NormalizedSuccessorsMap = info.successors
          .filter { case (next, _) => next != destination }
          .mapValues { presences => presences.toDouble / totalShortestPaths }
          .map(identity) // work-around to solve scala bug (https://github.com/scala/bug/issues/7005)
        (destination, successorsMap)
      }.filter { case (_, info) => info.nonEmpty }
      VertexData(0.0, updatedShortestPaths, Map.empty)
    }

    val vertexProgram: (VertexId, VertexData, ScoresMap) => VertexData =
      (_, vertexData, receivedScores) => {
        val newScore: Double = vertexData.score + receivedScores.values.sum
        VertexData(newScore, vertexData.shortestPaths, receivedScores)
      }

    val sendMessage: EdgeTriplet[VertexData, E] => Iterator[(VertexId, ScoresMap)] =
      triplet => {
        val nextId: VertexId = triplet.dstId
        val shortestPaths: ShortestPathsMap = triplet.srcAttr.shortestPaths
        val receivedScores: ScoresMap = triplet.srcAttr.latestReceivedScores
        /*
         * The map of received scores states how many nodes have given a score to the origin vertex in the previous
         * round those scores must be propagated forward until the destination, splitting the received value
         * respectively to the weight owned by each successor of the vertex through that specific destination.
         *
         * The mapping function maps each destination to the score that must be assigned to the nextId vertex
         * respectively to the path towards that specific destination (note that, as scores are propagated towards the
         * destination, we do not have information about the origin of the path that we are considering)
         *
         * Finally, the mapping function is different whether the received scores map is empty or not.
         * This is useful during the first iteration, as the vertex will receive an empty map for scores from the
         * Pregel API in order to start sending messages. In this specific case, indeed, we are sure that the vertices
         * we are considering are the origin of the paths, hence we will know that the score value is "initialized"
         * in this specific vertex and will have the value of 1.0 (namely a "complete" score that has not been split
         * into or merged from different paths yet)
         */
        val mappingFunction: (VertexId, NormalizedSuccessorsMap) => Double =
          if (receivedScores.isEmpty) (_, successorsMap) => successorsMap.getOrElse(nextId, 0.0)
          else (destinationId, successorsMap) => receivedScores.get(destinationId)
            .map(score => score * successorsMap.getOrElse(nextId, 0.0))
            .getOrElse(0.0)
        val scoresMap: ScoresMap = shortestPaths.map { case (destinationId, successorsMap) =>
          (destinationId, mappingFunction(destinationId, successorsMap))
        }.filter { case (_, score) => score != 0 }
        if (scoresMap.isEmpty) Iterator.empty else Iterator((nextId, scoresMap))
      }

    val mergeMessages: (ScoresMap, ScoresMap) => ScoresMap =
      (m, n) => m.merge(n, { case (_, mScores, nScores) => mScores + nScores })

    initialGraph.pregel[ScoresMap](initialMsg = Map.empty, activeDirection = EdgeDirection.Both)(
      vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
    ).mapVertices((_, vertexData) => vertexData.score)
  }
}

