package com.netflix.atlas.core.model

import munit.Assertions.assertEquals
import munit.FunSuite
import org.junit.Assert.assertFalse

class DatapointMetaSuite extends FunSuite {

  test("intersect: same ref") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val wrapper = LazyIntersection(a, a)
    assertEquals(wrapper.datapointMeta(0L).get.get("a").get, "1")
    assertEquals(wrapper.datapointMeta(0L).get.get("b").get, "2")
    assertEquals(wrapper.datapointMeta(1L).get.get("a").get, "3")
    assertEquals(wrapper.datapointMeta(1L).get.get("b").get, "4")
  }

  test("intersect: duplicate") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val b = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val wrapper = LazyIntersection(a, a)
    assertEquals(wrapper.datapointMeta(0L).get.get("a").get, "1")
    assertEquals(wrapper.datapointMeta(0L).get.get("b").get, "2")
    assertEquals(wrapper.datapointMeta(1L).get.get("a").get, "3")
    assertEquals(wrapper.datapointMeta(1L).get.get("b").get, "4")
  }

  test("intersect: intersect keys") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val b = new MapMeta(Map(0L -> Map("a" -> "1", "c" -> "2"), 1L -> Map("d" -> "3", "b" -> "4")))
    val wrapper = LazyIntersection(a, b)
    assertEquals(wrapper.datapointMeta(0L).get.get("a").get, "1")
    assertFalse(wrapper.datapointMeta(0L).get.get("b").isDefined)
    assertFalse(wrapper.datapointMeta(0L).get.get("c").isDefined)
    assertFalse(wrapper.datapointMeta(1L).get.get("a").isDefined)
    assertEquals(wrapper.datapointMeta(1L).get.get("b").get, "4")
  }

  test("intersect: intersect values") {
    val a = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "2"), 1L -> Map("a" -> "3", "b" -> "4")))
    val b = new MapMeta(Map(0L -> Map("a" -> "1", "b" -> "3"), 1L -> Map("a" -> "5", "b" -> "4")))
    val wrapper = LazyIntersection(a, b)
    assertEquals(wrapper.datapointMeta(0L).get.get("a").get, "1")
    assertFalse(wrapper.datapointMeta(0L).get.get("b").isDefined)
    assertFalse(wrapper.datapointMeta(1L).get.get("a").isDefined)
    assertEquals(wrapper.datapointMeta(1L).get.get("b").get, "4")
  }

  test("intersect: disjoint keys") {
    val a = new MapMeta(Map(0L -> Map("b" -> "2"), 1L -> Map("a" -> "3")))
    val b = new MapMeta(Map(0L -> Map("c" -> "2"), 1L -> Map("d" -> "3")))
    val wrapper = LazyIntersection(a, b)
    assertFalse(wrapper.datapointMeta(0L).isDefined)
    assertFalse(wrapper.datapointMeta(1L).isDefined)
  }
}

case class MetaWrapper(
  timeSeries: TimeSeries,
  metaData: List[String] = List("job", s"My job {i}"),
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

object MetaWrapper {

  def assertMeta(series: TimeSeries, timestamp: Long, metaData: List[String]): Unit = {
    var map = Map.newBuilder[String, String]
    for (i <- 0 until metaData.size by 2) {
      val key = metaData(i)
      val value = metaData(i + 1).replaceAll("\\{i\\}", timestamp.toString)
      map += key -> value
    }
    val expected = map.result()
    if (series.meta.isEmpty) {
      throw new AssertionError("no meta object")
    }
    val actual = series.meta.get.datapointMeta(timestamp).getOrElse {
      throw new AssertionError(s"no meta for $timestamp")
    }

    map = Map.newBuilder[String, String]
    actual.keys.foreach { k =>
      map += k -> actual.get(k).get
    }
    assertEquals(map.result(), expected)
  }

  def assertMeta(series: TimeSeries, timestamp: Long, metaData: DatapointMeta): Unit = {
    if (series.meta.isEmpty) {
      throw new AssertionError("no meta object")
    }
    val expMeta = metaData.datapointMeta(timestamp)
    val actualMeta = series.meta.get.datapointMeta(timestamp)
    (expMeta, actualMeta) match {
      case (Some(e), Some(a)) =>
        assertEquals(metaToMap(a), metaToMap(e), s"Mismatch at ${timestamp}")
      case (None, None) =>
      // all good
      case _ =>
        throw new AssertionError(
          s"meta mismatch: actual: $actualMeta != expected: $expMeta @ ${timestamp}"
        )
    }
  }

  def metaToMap(meta: DatapointMetaEntry): Map[String, String] = {
    val map = Map.newBuilder[String, String]
    meta.keys.foreach { k =>
      map += k -> meta.get(k).get
    }
    map.result()
  }
}
