package com.netflix.atlas.core.model

trait DatapointMetaEntry {

  def keys: List[String]

  def get(key: String): Option[String]

}

trait DatapointMeta {

  def datapointMeta(timestamp: Long): Option[DatapointMetaEntry] = None

}

class MapMetaEntry(map: Map[String, String]) extends DatapointMetaEntry {

  override def keys: List[String] = map.keys.toList

  override def get(key: String): Option[String] = map.get(key)

  override def toString: String =
    keys.map { case k => s"$k=${get(k).getOrElse("")}" }.mkString(", ")
}

object DatapointMeta {

  def consolidate(
    start: Long,
    end: Long,
    metaA: Option[DatapointMeta],
    metaB: Option[DatapointMeta]
  ): Option[DatapointMeta] = {
    (metaA, metaB) match {
      case (Some(a), Some(b)) =>
        // TODO - implement this
        Some(a)
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(b)
      case (None, None)    => None
    }
  }
}
