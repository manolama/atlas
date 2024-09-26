package com.netflix.atlas.core.model

import munit.FunSuite
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame

class DatapointMetaSuite extends FunSuite {

  test("intersect: same ref") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val result = DatapointMeta.intersect(0L, 2L, Some(a), Some(a))
    assertSame(a, result.get)
  }

  test("intersect: duplicate") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val b = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val result = DatapointMeta.intersect(0L, 2L, Some(a), Some(b))
    assertSame(a, result.get)
  }

  test("intersect: intersect keys") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val b = new MapMeta(Map(0L -> Map("a" -> "1", "c" -> "2"), 1L -> Map("d" -> "3", "b" -> "4")))
    val result = DatapointMeta.intersect(0L, 2L, Some(a), Some(b))
    assertEquals(result.get.datapointMeta(0L).get.get("a").get, "1")
    assertFalse(result.get.datapointMeta(0L).get.get("b").isDefined)
    assertFalse(result.get.datapointMeta(1L).get.get("a").isDefined)
    assertEquals(result.get.datapointMeta(1L).get.get("b").get, "4")
  }

  test("intersect: intersect values") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val b = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "3"), 1L -> Map("a" -> "5", "b" -> "4")))
    val result = DatapointMeta.intersect(0L, 2L, Some(a), Some(b))
    assertEquals(result.get.datapointMeta(0L).get.get("a").get, "1")
    assertFalse(result.get.datapointMeta(0L).get.get("b").isDefined)
    assertFalse(result.get.datapointMeta(1L).get.get("a").isDefined)
    assertEquals(result.get.datapointMeta(1L).get.get("b").get, "4")
  }

  test("intersect: disjoint keys") {
    val a = new MapMeta(Map(0L -> Map("b" -> "2"), 1L -> Map("a" -> "3")))
    val b = new MapMeta(Map(0L -> Map("c" -> "2"), 1L -> Map("d" -> "3")))
    val result = DatapointMeta.intersect(0L, 2L, Some(a), Some(b))
    assertFalse(result.isDefined)
  }
}

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
