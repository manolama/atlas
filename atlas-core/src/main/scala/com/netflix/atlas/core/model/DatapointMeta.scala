package com.netflix.atlas.core.model

import java.util
import scala.collection.mutable

trait DatapointMetaEntry {

  def keys: List[String]

  def get(key: String): Option[String]

}

trait DatapointMeta {

  def datapointMeta(timestamp: Long): Option[DatapointMetaEntry]

}

class MapMetaEntry(map: Map[String, String]) extends DatapointMetaEntry {

  override def keys: List[String] = map.keys.toList

  override def get(key: String): Option[String] = map.get(key)

  override def toString: String =
    keys.map { case k => s"$k=${get(k).getOrElse("")}" }.mkString(", ")
}

class MapMeta(data: Map[Long, Map[String, String]]) extends DatapointMeta {

  override def datapointMeta(timestamp: Long): Option[DatapointMetaEntry] = {
    data.get(timestamp.toInt).map(new MapMetaEntry(_))
  }

}

object DatapointMeta {

  def intersect(
    start: Long,
    end: Long,
    metaA: Option[DatapointMeta],
    metaB: Option[DatapointMeta]
  ): Option[DatapointMeta] = {
    (metaA, metaB) match {
      case (Some(a), Some(b)) =>
        checkForDifferences(start, end, a, b)
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(b)
      case (None, None)    => None
    }
  }

  private def checkForDifferences(
    start: Long,
    end: Long,
    metaA: DatapointMeta,
    metaB: DatapointMeta
  ): Option[DatapointMeta] = {
    // same ref, so no need to check
    if (metaA == metaB) {
      return Some(metaA)
    }

    // for series with meta, we currently assume a step of 1
    for (i <- start until end) {
      (metaA.datapointMeta(i), metaB.datapointMeta(i)) match {
        case (Some(a), Some(b)) =>
          if (a.keys != b.keys) {
            return merge(start, end, metaA, metaB)
          }
          a.keys.foreach { key =>
            if (a.get(key) != b.get(key)) {
              return merge(start, end, metaA, metaB)
            }
          }

        case (Some(a), None) =>
          if (a.keys.nonEmpty) {
            return merge(start, end, metaA, metaB)
          }

        case (None, Some(b)) =>
          if (b.keys.nonEmpty) {
            return merge(start, end, metaA, metaB)
          }
        case (None, None) => // No-op
      }
    }
    // same so just return one ref.
    Some(metaA)
  }

  private def merge(
    start: Long,
    end: Long,
    metaA: DatapointMeta,
    metaB: DatapointMeta
  ): Option[DatapointMeta] = {
    val datapointMeta = Map.newBuilder[Long, Map[String, String]]
    // for series with meta, we currently assume a step of 1
    for (i <- start until end) {
      var innerMap: mutable.Builder[(String, String), Map[String, String]] = null
      (metaA.datapointMeta(i), metaB.datapointMeta(i)) match {
        case (Some(a), Some(b)) =>
          val intersection = a.keys.intersect(b.keys)
          intersection.foreach { key =>
            if (a.get(key) == b.get(key)) {
              if (innerMap == null) {
                innerMap = Map.newBuilder[String, String]
              }
              innerMap += key -> a.get(key).get
            }
          }
        case (Some(_), None) =>
        case (None, Some(_)) =>
        case (None, None)    => // No-op
      }
      if (innerMap != null) {
        datapointMeta += i -> innerMap.result()
      }
    }

    val results = datapointMeta.result()
    if (results.isEmpty) {
      None
    } else {
      Some(new MapMeta(results))
    }
  }
}
