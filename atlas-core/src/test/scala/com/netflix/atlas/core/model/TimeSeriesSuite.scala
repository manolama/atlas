package com.netflix.atlas.core.model

case class MetaWrapper(
  timeSeries: TimeSeries,
  metaData: List[String],
  metaSkips: List[Boolean] = Nil
) extends TimeSeries {

  class MetaImpl extends DatapointMeta {

    override def datapointMeta(timestamp: Long): Option[DatapointMetaEntry] = {
      if (metaSkips.nonEmpty && !metaSkips(timestamp.toInt)) {
        return None
      }

      val map = Map.newBuilder[String, String]
      for (i <- 0 until metaData.size by 2) {
        val key = metaData(i)
        val value = metaData(i + 1).replaceAll("\\{i\\}", timestamp.toString)
        map += key -> value
      }
      Some(new MapMetaEntry(map.result()))
    }
  }

  override def meta: Option[DatapointMeta] = Some(new MetaImpl)

  override def label: String = timeSeries.label

  override def data: TimeSeq = timeSeries.data

  /** Unique id based on the tags. */
  override def id: ItemId = timeSeries.id

  /** The tags associated with this item. */
  override def tags: Map[String, String] = timeSeries.tags
}

class TimeSeriesSuite {}
