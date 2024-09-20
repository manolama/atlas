package com.netflix.atlas.chart

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.util.GraphAssertions
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.BasicTimeSeries
import com.netflix.atlas.core.model.DatapointMeta
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.TimeSeq
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import java.awt.Color
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util
import scala.concurrent.duration.DurationInt

class FooSuite extends BasePngGraphEngineSuite {

  protected val start: Long = 1704067200000L

  override def step: Long = 1

  test("foo") {
    val line = LineDef(series1)
    val line2 = LineDef(series2, color = Color.GREEN)
    val line3 = LineDef(shortSeries, color = Color.BLUE)
    val plotDef = PlotDef(List(line, line3, line2))
    val graphDef = GraphDef(
      width = 480,
      startTime = Instant.ofEpochMilli(0),
      endTime = Instant.ofEpochMilli(5),
      step = 1,
      plots = List(plotDef),
      stepless = true
    )
    check("_FOO.png", graphDef)
  }

  test("wave") {
    val series1 = MetaWrapper(wave(0, 1, Duration.ofMillis(5)), List("job", "Batch job {i}"))
    val line = LineDef(series1, groupByKeys = List("k1"))
    val plotDef = PlotDef(List(line))
    val graphDef = GraphDef(
      width = 480,
      startTime = Instant.ofEpochMilli(0),
      endTime = Instant.ofEpochMilli(30),
      step = step,
      plots = List(plotDef),
      stepless = true
    )
    check("_FOO.png", graphDef)
  }

//  def singleSeries(
//    series: TimeSeries,
//
//                  )

  def series1: TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(1.0, 2.0, 3.0, 4.0, 5.0))
    TSWithMeta(
      Map("k1" -> "v1"),
      "some.series",
      seq,
      List(
        Map(
          "job" -> "Batch job a",
          "ts" -> "1704068642000   asdfasdfadsfasadfadsfjlkajsdfslkjasdlkfjalskdfjfalskdjflkjasddlfkjasldkfjlkjasldkijfliasmldfimasldidmflaimsfdlimdslafimlasidmfloisdmf"
        ),
        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
        Map("job" -> "Batch job c", "ts" -> "1704080642000"),
        Map("job" -> "Batch job d", "ts" -> "1704090907000"),
        Map("job" -> "Batch job e", "ts" -> "1704092439000")
      )
    )
  }

  def series2: TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(6.0, 4.0, 2.0, 5.0, 3.0))
    TSWithMeta(
      Map("k1" -> "v1"),
      "second.metric",
      seq,
      List(
        Map("job" -> "Batch job a", "ts" -> "1704068642000"),
        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
        Map("job" -> "Batch job c", "ts" -> "1704080642000"),
        Map("job" -> "Batch job d", "ts" -> "1704090907000"),
        Map("job" -> "Batch job e", "ts" -> "1704092439000")
      )
    )
  }

  def shortSeries: TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(3.5, 3.0))
    TSWithMeta(
      Map("k1" -> "v1"),
      "short.dataset",
      seq,
      List(
        Map("job" -> "Batch job a", "ts" -> "1704068642000"),
        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
        Map("job" -> "Batch job c", "ts" -> "1704080642000")
      )
    )
  }

//  def series1: IregTS = {
//    IregTS(
//      start,
//      "some.series",
//      Map("k1" -> "v1"),
//      List(
//        Map("job" -> "Batch job a", "ts" -> "1704068642000"),
//        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
//        Map("job" -> "Batch job c", "ts" -> "1704080642000"),
//        Map("job" -> "Batch job d", "ts" -> "1704090907000"),
//        Map("job" -> "Batch job e", "ts" -> "1704092439000")
//      ),
//      Array(
//        1.0, 2.0, 3.0, 4.0, 5.0
//      )
//    )
//  }
}

case class MetaWrapper(
  timeSeries: TimeSeries,
  meta: List[String]
) extends TimeSeries {

  override def datapointMeta(timestamp: Long): Option[DatapointMeta] = {
    val map = Map.newBuilder[String, String]
    for (i <- 0 until meta.size by 2) {
      val key = meta(i)
      val value = meta(i + 1).replaceAll("\\{i\\}", timestamp.toString)
      map += key -> value
    }
    Some(new MapMeta(map.result()))
  }

  override def label: String = timeSeries.label

  override def data: TimeSeq = timeSeries.data

  /** Unique id based on the tags. */
  override def id: ItemId = timeSeries.id

  /** The tags associated with this item. */
  override def tags: Map[String, String] = timeSeries.tags
}

case class TSWithMeta(
  tags: Map[String, String],
  label: String,
  data: TimeSeq,
  meta: List[Map[String, String]]
) extends TimeSeries {

  /** Unique id based on the tags. */
  override def id: ItemId = ???

  override def datapointMeta(timestamp: Long): Option[DatapointMeta] = {
    if (timestamp >= meta.size) {
      None
    } else {
      Some(new MapMeta(meta(timestamp.toInt)))
    }
  }
}

class MapMeta(map: Map[String, String]) extends DatapointMeta {

  override def keys: List[String] = map.keys.toList

  override def get(key: String): Option[String] = map.get(key)
}
