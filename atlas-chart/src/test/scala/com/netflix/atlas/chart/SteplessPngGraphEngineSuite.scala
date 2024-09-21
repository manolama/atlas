package com.netflix.atlas.chart

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotBound.Explicit
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
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util
import scala.concurrent.duration.DurationInt

class SteplessPngGraphEngineSuite extends BasePngGraphEngineSuite {

  protected val start: Long = 1704067200000L

  override def step: Long = 1

  private val palette = Palette.fromResource("armytage")
  private val meta = List("job", "Batch job {i}", "ts", "{i}")

  def singleSeriesTest(
    name: String,
    series: TimeSeries,
    meta: List[String],
    metaSkips: List[Boolean] = Nil,
    endTime: Long = 30,
    f: GraphDef => GraphDef = identity
  ): Unit = {
    test(name) {
      val series1 = MetaWrapper(series, meta, metaSkips)
      val line = LineDef(series1)
      val plotDef = PlotDef(List(line))
      val graphDef = f.apply(
        GraphDef(
          width = 480,
          startTime = Instant.ofEpochMilli(0),
          endTime = Instant.ofEpochMilli(endTime),
          step = step,
          plots = List(plotDef),
          stepless = true
        )
      )
      check(s"${name}.png", graphDef)
    }
  }

  singleSeriesTest("single_constant", constant(42.5), meta)

  singleSeriesTest(
    "single_wave",
    wave(0, 1, Duration.ofMillis(5)),
    meta
  )

  singleSeriesTest(
    "single_truncated_legend",
    wave(0, 1, Duration.ofMillis(5)),
    List("job", "Batch job {i}", "ts", "{i}", "longValue", String.format("%1024s", "Foo"))
  )

  singleSeriesTest(
    "single_wave_wide",
    wave(0, 1, Duration.ofMillis(5)),
    meta,
    endTime = 1024
  )

  singleSeriesTest(
    "single_wave_stacked",
    wave(0, 1, Duration.ofMillis(5)),
    meta,
    f = v => v.adjustLines(_.copy(lineStyle = LineStyle.STACK))
  )

  singleSeriesTest(
    "single_metadata_gaps",
    seriesFromArray("timeseries", Array(1.0, 1.5, 2.0, 1.0, 3.0)),
    meta,
    List(true, false, false, true, false),
    endTime = 5
  )

  singleSeriesTest(
    "single_nans",
    seriesFromArray("nans", Array(1.0, Double.NaN, 2.0, Double.NaN, 3.0)),
    meta,
    endTime = 5
  )

  singleSeriesTest(
    "single_upper",
    wave(0, 1, Duration.ofMillis(5)),
    meta,
    f = v => v.adjustPlots(_.copy(upper = Explicit(0.5)))
  )

  singleSeriesTest(
    "single_lower",
    wave(0, 1, Duration.ofMillis(5)),
    meta,
    f = v => v.adjustPlots(_.copy(lower = Explicit(0.5)))
  )

  singleSeriesTest(
    "single_upper_and_lower",
    wave(0, 1, Duration.ofMillis(5)),
    meta,
    f = v => v.adjustPlots(_.copy(upper = Explicit(0.75), lower = Explicit(0.25)))
  )

  test("single_multix") {
    val series1 = MetaWrapper(wave(0, 1, Duration.ofMillis(5)), meta)
    val line = LineDef(series1)
    val plotDef = PlotDef(List(line))
    val graphDef = GraphDef(
      width = 480,
      startTime = Instant.ofEpochMilli(0),
      endTime = Instant.ofEpochMilli(30),
      step = step,
      plots = List(plotDef),
      stepless = true,
      timezones = List(ZoneOffset.UTC, ZoneId.of("America/Los_Angeles"))
    )
    check("single_multix.png", graphDef)
  }

  def multiSeriesTest(
    name: String,
    series: Array[TimeSeries],
    meta: List[String],
    metaSkips: List[List[Boolean]] = Nil,
    endTime: Long = 30,
    f: GraphDef => GraphDef = identity
  ): Unit = {
    test(name) {
      val lines = series.zipWithIndex
        .map { tuple =>
          val skips = if (metaSkips.nonEmpty) metaSkips(tuple._2) else Nil
          (MetaWrapper(tuple._1, meta, skips), tuple._2)
        }
        .map(tuple => LineDef(tuple._1, color = palette.colors(tuple._2)))
        .toList
      val plotDef = PlotDef(lines)
      val graphDef = f.apply(
        GraphDef(
          width = 480,
          startTime = Instant.ofEpochMilli(0),
          endTime = Instant.ofEpochMilli(endTime),
          step = step,
          plots = List(plotDef),
          stepless = true
        )
      )
      check(s"${name}.png", graphDef)
    }
  }

  multiSeriesTest(
    "multi_wave",
    Array(
      wave(.5, 1, Duration.ofMillis(5)),
      wave(0, 2, Duration.ofMillis(10)),
      constant(1.0)
    ),
    meta
  )

  multiSeriesTest(
    "multi_wave_stacked",
    Array(
      wave(.5, 1, Duration.ofMillis(5)),
      wave(0, 2, Duration.ofMillis(10)),
      constant(1.0)
    ),
    meta,
    f = v => v.adjustLines(_.copy(lineStyle = LineStyle.STACK))
  )

  multiSeriesTest(
    "multi_wave_wide",
    Array(
      wave(.5, 1, Duration.ofMillis(5)),
      wave(0, 2, Duration.ofMillis(10)),
      constant(1.0)
    ),
    meta,
    endTime = 1024
  )

  multiSeriesTest(
    "multi_nans",
    Array(
      seriesFromArray("nans1", Array(1.0, Double.NaN, 2.0, Double.NaN, 3.0)),
      seriesFromArray("nans2", Array(Double.NaN, 5.0, 2.5, Double.NaN, Double.NaN)),
      seriesFromArray("nans3", Array(Double.NaN, 1.5, Double.NaN, Double.NaN, Double.NaN))
    ),
    meta,
    endTime = 5
  )

  multiSeriesTest(
    "multi_wave_metadata_gaps",
    Array(
      wave(.5, 1, Duration.ofMillis(5)),
      wave(0, 2, Duration.ofMillis(10)),
      constant(1.0)
    ),
    meta,
    List(List(true, false, false, true, false), Nil, List(false, true, false, false, true)),
    endTime = 5
  )

  test("multi_wave_multiy") {
    val series = Array(
      wave(.5, 1, Duration.ofMillis(5)),
      wave(0, 2, Duration.ofMillis(10)),
      constant(1.0)
    )

    val plotDefs = series.zipWithIndex
      .map(t => (MetaWrapper(t._1, meta), t._2))
      .map(t => PlotDef(List(LineDef(t._1, color = palette.colors(t._2)))))
      .toList
    val graphDef = GraphDef(
      width = 480,
      startTime = Instant.ofEpochMilli(0),
      endTime = Instant.ofEpochMilli(30),
      step = step,
      plots = plotDefs,
      stepless = true
    )
    check("multi_wave_multiy.png", graphDef)
  }

  def seriesFromArray(name: String, data: Array[Double]): TimeSeries = {
    BasicTimeSeries(null, Map.empty, name, new ArrayTimeSeq(DsType.Gauge, 0, 1, data))
  }

}

case class MetaWrapper(
  timeSeries: TimeSeries,
  meta: List[String],
  metaSkips: List[Boolean] = Nil
) extends TimeSeries {

  override def datapointMeta(timestamp: Long): Option[DatapointMeta] = {
    if (metaSkips.nonEmpty && !metaSkips(timestamp.toInt)) {
      return None
    }

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

class MapMeta(map: Map[String, String]) extends DatapointMeta {

  override def keys: List[String] = map.keys.toList

  override def get(key: String): Option[String] = map.get(key)
}
