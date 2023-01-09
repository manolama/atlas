package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.HeatMap.singleColorAlphas
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model.PlotBound.Explicit
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.HeatmapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.index.DataSet.wave
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import java.time.Duration
import java.time.Instant
import java.util

class BasicHeatMapSuite extends FunSuite {

  val start = 1672819200000L
  val end = start + (60_000 * 60)

  test("1 series positive value") {
    val dps = new Array[Double](60)
    util.Arrays.fill(dps, 1)
    val heatmap = generateHeatmap(List(dps))
    assertEquals(heatmap.rows.size, 1)
    assertRowCounts(2, 1, heatmap.rows.head)
    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 1, 1),
        (singleColorAlphas(3), 2, 2)
      )
    )
    assertEquals(heatmap.label, "query")
  }

  test("1 series negative value") {
    val dps = new Array[Double](60)
    util.Arrays.fill(dps, -1)
    val heatmap = generateHeatmap(List(dps))
    assertEquals(heatmap.rows.size, 1)
    assertRowCounts(2, 1, heatmap.rows.head)
    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 1, 1),
        (singleColorAlphas(3), 2, 2)
      )
    )
  }

  test("1 series values of 0") {
    val dps = new Array[Double](60)
    val heatmap = generateHeatmap(List(dps))
    assertEquals(heatmap.rows.size, 1)
    assertRowCounts(2, 1, heatmap.rows.head)
    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 1, 1),
        (singleColorAlphas(3), 2, 2)
      )
    )
  }

  test("4 series w 1 overlap") {
    val dpsA = new Array[Double](60)
    util.Arrays.fill(dpsA, 1)
    val dpsB = new Array[Double](60)
    util.Arrays.fill(dpsB, 10)
    val dpsC = new Array[Double](60)
    util.Arrays.fill(dpsC, 15)
    val heatmap = generateHeatmap(List(dpsA, dpsB, dpsB, dpsC))
    assertEquals(heatmap.rows.size, 15)
    for (i <- 0 until heatmap.rows.size) {
      val row = heatmap.rows(i)
      i match {
        case 0  => assertRowCounts(2, 1, row)
        case 9  => assertRowCounts(4, 2, row)
        case 14 => assertRowCounts(2, 1, row)
        case _  => assertRowCounts(0, 0, row)
      }
    }
    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 1, 1),
        (singleColorAlphas(5), 2, 2),
        (singleColorAlphas(1), 4, 4)
      )
    )
  }

  test("Explicit bounds") {
    val dpsA = new Array[Double](60)
    util.Arrays.fill(dpsA, 1)
    val dpsB = new Array[Double](60)
    util.Arrays.fill(dpsB, 10)
    val dpsC = new Array[Double](60)
    util.Arrays.fill(dpsC, 15)
    val heatmap = generateHeatmap(List(dpsA, dpsB, dpsB, dpsC), Some(2.0))
    assertEquals(heatmap.rows.size, 15)
    for (i <- 0 until heatmap.rows.size) {
      val row = heatmap.rows(i)
      i match {
        case 0  => assertRowCounts(2, 0, row)
        case 9  => assertRowCounts(4, 2, row)
        case 14 => assertRowCounts(2, 0, row)
        case _  => assertRowCounts(0, 0, row)
      }
    }
    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 2, 2),
        (singleColorAlphas(2), 4, 4)
      )
    )
  }

  test("2 waves") {
    val ts1 = wave(1, 1024, Duration.ofHours(1))
    val ts2 = wave(256, 768, Duration.ofHours(1))

    val heatmap = generateHeatmapSeries(List(ts1, ts2))
    assertEquals(heatmap.rows.size, 10)
    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 1, 1),
        (singleColorAlphas(5), 2, 2),
        (singleColorAlphas(3), 3, 3),
        (singleColorAlphas(1), 4, 4)
      )
    )
  }

  test("legend label") {
    val dps = new Array[Double](60)
    util.Arrays.fill(dps, 1)
    val heatmap = generateHeatmap(List(dps), label = "My Heatmap")
    assertEquals(heatmap.rows.size, 1)
    assertRowCounts(2, 1, heatmap.rows.head)
    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 1, 1),
        (singleColorAlphas(3), 2, 2)
      )
    )
    assertEquals(heatmap.label, "My Heatmap")
  }

  def generateHeatmap(
    data: List[Array[Double]],
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None,
    label: String = "query"
  ): BasicHeatMap = {
    var idx = 0
    val timeseries = data.map { dps =>
      idx += 1
      TimeSeries(
        Map("series" -> idx.toString),
        label,
        new ArrayTimeSeq(DsType.Gauge, 1672819200000L, 60_000, dps)
      )
    }
    generateHeatmapSeries(timeseries, lowerBound, upperBound)
  }

  def generateHeatmapSeries(
    data: List[TimeSeries],
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None
  ): BasicHeatMap = {
    var min = Double.MaxValue
    var max = Double.MinValue
    var idx = 0
    val timeseries = data.map { ts =>
      ts.data.foreach(start, end) { (t, dp) =>
        if (dp < min) min = dp
        if (dp > max) max = dp
      }

      idx += 1
      LineDef(
        ts,
        lineStyle = LineStyle.HEATMAP
      )
    }

    val plotDef = if (lowerBound.nonEmpty || upperBound.nonEmpty) {
      PlotDef(
        timeseries,
        heatmapDef = Some(
          HeatmapDef(
            lower = if (lowerBound.nonEmpty) Explicit(lowerBound.get) else AutoStyle,
            upper = if (upperBound.nonEmpty) Explicit(upperBound.get) else AutoStyle
          )
        )
      )
    } else {
      PlotDef(timeseries)
    }
    val graphDef = GraphDef(
      List(plotDef),
      Instant.ofEpochMilli(start),
      Instant.ofEpochMilli(end)
    )
    val yaxis = LeftValueAxis(plotDef, graphDef.theme.axis, min, max)
    val timeAxis = TimeAxis(
      Style(color = graphDef.theme.axis.line.color),
      graphDef.startTime.toEpochMilli,
      graphDef.endTime.toEpochMilli,
      graphDef.step,
      graphDef.timezone,
      40
    )
    BasicHeatMap(
      graphDef,
      plotDef,
      yaxis,
      timeAxis,
      x1 = 0,
      y1 = 0,
      x2 = 400,
      chartEnd = 200
    )
  }

  def assertColorMap(
    obtained: List[HeatMapLegendColor],
    expected: List[(Int, Double, Double)]
  ): Unit = {
    assertEquals(obtained.size, expected.size)
    var idx = 0
    obtained.zip(expected).foreach { oe =>
      val (c, (alpha, min, max)) = oe
      assertEquals(c.color.getAlpha, alpha, s"Incorrect alpha at ${idx}")
      assertEquals(c.min, min, s"Incorrect min at ${idx}")
      assertEquals(c.max, max, s"Incorrect max at ${idx}")
      idx += 1
    }
  }

  def assertRowCounts(v: Double, min: Double, row: Array[Double]): Unit = {
    assertEquals(row.head, min)
    for (i <- 1 until row.length - 1) assertEquals(row(i), v)
    assertEquals(row.last, min)
  }
}
