package com.instagraph.indices.centrality

import com.instagraph.paths.ShortestPaths.AllPairs
import com.instagraph.utils.MapUtils.Manipulations
import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

case class BetweennessCentrality[E: ClassTag](implicit numeric: Numeric[E]) extends CentralityIndex[Double, E] {
  type NormalizedSuccessorsMap = Map[VertexId, Double]
  type ShortestPathsMap = Map[VertexId, NormalizedSuccessorsMap]
  type ScoresMap = Map[VertexId, Double]

  private case class VertexData(score: Double, shortestPaths: ShortestPathsMap, latestReceivedScores: ScoresMap)

  override def compute[V: ClassTag](graph: Graph[V, E]): Graph[Double, E] = {
    val initialGraph: Graph[VertexData, E] = graph.eachSuccessorWeightedAPSP.mapVertices { (_, shortestPaths) =>
      val updatedShortestPaths: ShortestPathsMap = shortestPaths.map { case (destination, info) =>
        val totalShortestPaths: Int = info.adjacentVertices.values.sum
        val successorsMap: NormalizedSuccessorsMap = info.adjacentVertices
          .filter { case (next, _) => next != destination }
          .mapValues { presences => presences.toDouble / totalShortestPaths }
          .map(identity) // https://github.com/scala/bug/issues/7005
        (destination, successorsMap)
      }.filter { case (_, info) => info.nonEmpty }
      VertexData(0.0, updatedShortestPaths, Map.empty)
    }

    val vertexProgram: (VertexId, VertexData, ScoresMap) => VertexData =
      (_, vertexData, receivedScores) => {
        val newScore: Double = vertexData.score + receivedScores.values.sum
        /*
         * During the first iteration each vertex should get, as initial message, the map of each destination it can
         * reach linked to the score of 1.0, namely a "complete" score that has not been split into or merged from
         * different paths yet. Though, as the Pregel API allow to send a single message that is the same for each
         * vertex, we decided to send an empty map and check that case (in no other iterations the vertex will receive
         * an empty map, as a message will be sent if and only if the scores map contains at least one record) and
         * and return the actual initial scores map instead.
         */
        val latestReceivedScores: ScoresMap =
          if (receivedScores.nonEmpty) receivedScores
          else vertexData.shortestPaths.mapValues(_ => 1.0).map(identity) // https://github.com/scala/bug/issues/7005
        VertexData(newScore, vertexData.shortestPaths, latestReceivedScores)
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
         * The mapping function to get the score for each destination maps each destination to the score that must be
         * assigned to the nextId vertex respectively to the path towards that specific destination (note that, as
         * scores are propagated towards the destination, we do not have information about the origin of the path that
         * we are considering)
         */
        val scoresMap: ScoresMap = shortestPaths.map { case (destinationId, successorsMap) =>
          val score: Double = receivedScores.get(destinationId)
            .map(score => score * successorsMap.getOrElse(nextId, 0.0))
            .getOrElse(0.0)
          (destinationId, score)
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

