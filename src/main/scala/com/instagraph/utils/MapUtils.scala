package com.instagraph.utils

object MapUtils {
  implicit class Manipulations[K, V](map: Map[K, V]) {
    def merge(other: Map[K, V], mergingFunction: (K, V, V) => V): Map[K, V] = {
      map ++ other.map { case (key, otherValue) =>
        val mapValue: Option[V] = map.get(key)
        val updatedValue: V = if (mapValue.isEmpty) otherValue else mergingFunction(key, mapValue.get, otherValue)
        (key, updatedValue)
      }
    }
  }
}
