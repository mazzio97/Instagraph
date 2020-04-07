package com.instagraph.indices.centrality

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

case class PageRank[E: ClassTag]() extends CentralityIndex[Double, E] {
  private case class VertexData(score: Double, outDegrees: Int, toleranceReached: Boolean)

  override def compute[V: ClassTag](graph: Graph[V, E]): Graph[Double, E] = {
    val tolerance: Double = 1e-4
    val dampingFactor: Double = 0.15

    val initialGraph: Graph[VertexData, E] = graph
      .outerJoinVertices(graph.outDegrees)((_, _, outDegrees) => outDegrees.getOrElse(0))
      .mapVertices((_, outDegrees) => VertexData(0.0, outDegrees, toleranceReached = false))

    val vertexProgram: (VertexId, VertexData, Double) => VertexData =
      (_, status, adjacentSummation) => {
        val newScore: Double = dampingFactor + (1.0 - dampingFactor) * adjacentSummation
        VertexData(newScore, status.outDegrees, Math.abs(newScore - status.score) <= tolerance)
      }

    val sendMessage: EdgeTriplet[VertexData, E] => Iterator[(VertexId, Double)] = triplet => {
      val sourceData: VertexData = triplet.srcAttr
      val destinationData: VertexData = triplet.dstAttr
      val destinationId: VertexId = triplet.dstId
      if (destinationData.toleranceReached) Iterator.empty
      else Iterator((destinationId, sourceData.score / sourceData.outDegrees))
    }

    val mergeMessages: (Double, Double) => Double =
      (m, n) => m + n

    val finalGraph: Graph[VertexData, E] =
      initialGraph.pregel[Double](initialMsg = 0.0, activeDirection = EdgeDirection.In)(
        vprog = vertexProgram, sendMsg = sendMessage, mergeMsg = mergeMessages
      )

    val normalizationFactor = finalGraph.numVertices / finalGraph.vertices.values.map(data => data.score).sum()

    finalGraph.mapVertices { case (_, data) => data.score * normalizationFactor }
  }
}
