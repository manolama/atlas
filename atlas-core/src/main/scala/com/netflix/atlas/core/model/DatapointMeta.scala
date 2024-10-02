package com.netflix.atlas.core.model

import com.netflix.atlas.core.model.DatapointMeta.checkForDifferences

trait DatapointMetaEntry {

  def keys: List[String]

  def get(key: String): Option[String]

}

trait DatapointMeta {

  def datapointMeta(timestamp: Long): Option[DatapointMetaEntry]

}

case class OffsetMeta(offset: Long, meta: DatapointMeta) extends DatapointMeta {

  override def datapointMeta(timestamp: Long): Option[DatapointMetaEntry] = {
    meta.datapointMeta(timestamp - offset)
  }

}

class MapMetaEntry(map: Map[String, String]) extends DatapointMetaEntry {

  override def keys: List[String] = map.keys.toList

  override def get(key: String): Option[String] = map.get(key)

  override def toString: String =
    keys.map { case k => s"$k=${get(k).getOrElse("")}" }.mkString(", ")
}

case class MapMeta(data: Map[Long, Map[String, String]]) extends DatapointMeta {

  override def datapointMeta(timestamp: Long): Option[DatapointMetaEntry] = {
    data.get(timestamp.toInt).map(new MapMetaEntry(_))
  }

}

case class LazyIntersection(
  metaA: DatapointMeta,
  metaB: DatapointMeta
) extends DatapointMeta {

  override def datapointMeta(timestamp: Long): Option[DatapointMetaEntry] = {
    (metaA.datapointMeta(timestamp), metaB.datapointMeta(timestamp)) match {
      case (Some(a), Some(b)) => checkForDifferences(a, b)
      case _                  => None
    }
  }

}

object DatapointMeta {

  def intersect(
    metaA: Option[DatapointMeta],
    metaB: Option[DatapointMeta]
  ): Option[DatapointMeta] = {
    (metaA, metaB) match {
      case (Some(a), Some(b)) => Some(LazyIntersection(a, b))
      case _                  => None
    }
  }

  def checkForDifferences(
    a: DatapointMetaEntry,
    b: DatapointMetaEntry
  ): Option[DatapointMetaEntry] = {
    // May not be worth it, but the assumption here is that we're dealing with metrics
    // for the same set of meta. In that case we just validate the set is the same
    // without creating a new object.
    if (a.keys != b.keys) {
      return merge(a, b)
    }
    a.keys.foreach { key =>
      if (a.get(key) != b.get(key)) {
        return merge(a, b)
      }
    }
    Some(a)
  }

  private def merge(
    a: DatapointMetaEntry,
    b: DatapointMetaEntry
  ): Option[DatapointMetaEntry] = {
    val builder = Map.newBuilder[String, String]
    val intersection = a.keys.intersect(b.keys)
    intersection.foreach { key =>
      if (a.get(key) == b.get(key)) {
        builder += key -> a.get(key).get
      }
    }
    val results = builder.result()
    if (results.isEmpty) {
      None
    } else {
      Some(new MapMetaEntry(results))
    }
  }
}
