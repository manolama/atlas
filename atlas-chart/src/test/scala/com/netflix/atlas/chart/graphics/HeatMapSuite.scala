package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.HeatMap.choosePalette
import com.netflix.atlas.chart.graphics.HeatMap.colorScaler
import com.netflix.atlas.chart.graphics.HeatMap.singleColorAlphas
import com.netflix.atlas.chart.graphics.HeatMapSuite.start
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.isSpectatorPercentile
import com.netflix.atlas.chart.graphics.Scales.DoubleScale
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model.PlotBound.Explicit
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.HeatMapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Strings
import munit.Assertions.assertEquals
import munit.FunSuite

import java.awt.Color
import java.awt.Graphics2D
import java.time.Instant

class HeatMapSuite extends FunSuite {

  private val ts = TimeSeries(
    Map.empty,
    new ArrayTimeSeq(DsType.Gauge, start, 60_000, new Array[Double](59))
  )
  private val palette = Palette.create("blues")

  test("choosePalette single color") {
    val line = LineDef(ts)
    val palette = choosePalette(line)
    assertEquals(palette.uniqueColors.length, singleColorAlphas.length)
    for (i <- 0 until singleColorAlphas.length) {
      assertEquals(palette.uniqueColors(i).getAlpha, singleColorAlphas(i))
    }
  }

  test("choosePalette palette provided") {
    val line = LineDef(ts, palette = Some(Palette.create("blues")))
    val palette = choosePalette(line)
    assertEquals(palette, Palette.create("blues"))
  }

  test("choosePalette palette provided with single color") {
    val line = LineDef(ts, palette = Some(Palette.create("colors:A1D99B")))
    val palette = choosePalette(line)
    assertEquals(palette.uniqueColors.length, singleColorAlphas.length)
    for (i <- 0 until singleColorAlphas.length) {
      assertEquals(palette.uniqueColors(i).getAlpha, singleColorAlphas(i))
    }
  }

  test("colorScaler linear 1 to 1") {
    val plotDef = PlotDef(List.empty, scale = Scale.LINEAR)
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), i - 1)
    }
  }

  test("colorScaler linear 1 to larger") {
    val plotDef = PlotDef(List.empty, scale = Scale.LINEAR)
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length * 2)
    for (i <- 1 to palette.uniqueColors.length * 2 by 2) {
      assertEquals(scaler(i), (i - 1) / 2)
    }
  }

  test("colorScaler linear 1 to smaller") {
    val plotDef = PlotDef(List.empty)
    val scaler = colorScaler(plotDef, palette, 1, 3)
    assertEquals(scaler(1), 0)
    assertEquals(scaler(2), 2)
    assertEquals(scaler(3), 4)
  }

  test("colorScaler log") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatMapDef(colorScale = Scale.LOGARITHMIC)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    val expected = Array(0, 1, 3, 4, 5, 5, 6)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), expected(i - 1))
    }
  }

  test("colorScaler power 2") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatMapDef(colorScale = Scale.POWER_2)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    val expected = Array(0, 0, 0, 1, 2, 3, 5)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), expected(i - 1))
    }
  }

  test("colorScaler square root") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatMapDef(colorScale = Scale.SQRT)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), i - 1)
    }
  }

  test("colorScaler percentile switches to log") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatMapDef(colorScale = Scale.PERCENTILE)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    val expected = Array(0, 1, 3, 4, 5, 5, 6)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), expected(i - 1))
    }
  }

  test("colorScaler small values to linear") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatMapDef(colorScale = Scale.LOGARITHMIC)))
    val scaler = colorScaler(plotDef, palette, 0.001, 0.01)
    val expected = Array(0, 0, 1, 1, 2, 3, 3, 4, 5, 6)
    var count = 0.001
    for (i <- 1 to 10) {
      assertEquals(scaler(count), expected(i - 1))
      count += 0.001
    }
  }

  test("colorMap no real values processed") {
    val hm = MockHeatMap(palette.uniqueColors.length)
    assert(hm.colorMap.isEmpty)
  }

  test("colorMap all used with one value per color") {
    val hm = MockHeatMap(palette.uniqueColors.length)
    for (i <- 1 to palette.uniqueColors.length) {
      hm.updateLegend(i, hm.colorScaler(i))
    }

    val cm = hm.colorMap
    for (i <- 0 until cm.size) {
      assertEquals(cm(i).color, palette.uniqueColors.reverse(i))
      assertEquals(cm(i).min.toInt, i + 1)
      assertEquals(cm(i).max.toInt, i + 1)
    }
  }

  test("colorMap partial use") {
    val hm = MockHeatMap(3)
    for (i <- 1 to 3) {
      hm.updateLegend(i, hm.colorScaler(i))
    }

    val cm = hm.colorMap
    var paletteIndex = 0
    for (i <- 0 until 3) {
      val entry = cm(i)
      assertEquals(entry.color, palette.uniqueColors.reverse(paletteIndex))
      assertEquals(entry.min.toInt, i + 1)
      assertEquals(entry.max.toInt, i + 1)
      paletteIndex += 2
    }
  }

  test("colorMap diff min/max") {
    val hm = MockHeatMap(palette.uniqueColors.length * 2)
    for (i <- 1 to palette.uniqueColors.length * 2) {
      hm.updateLegend(i, hm.colorScaler(i))
    }

    val cm = hm.colorMap
    var paletteIndex = 0
    var min = 1
    cm.foreach { entry =>
      assertEquals(entry.color, palette.uniqueColors.reverse(paletteIndex))
      assertEquals(entry.min.toInt, min)
      assertEquals(entry.max.toInt, min + 1)
      min += 2
      paletteIndex += 1
    }
  }

  case class MockHeatMap(upperCellBound: Int) extends HeatMap {

    override def draw(g: Graphics2D): Unit = ???

    override def legendLabel: String = ???

    override def `type`: String = ???

    override def yticks: List[ValueTick] = ???

    override def rows: Array[Array[Double]] = ???

    override def palette: Palette = HeatMapSuite.this.palette

    override def colorScaler: DoubleScale =
      HeatMap.colorScaler(PlotDef(List.empty), palette, 1, upperCellBound)
  }
}

object HeatMapSuite {

  val start = 1672819200000L
  val end = start + (60_000 * 60)

  def generateHeatmap(
    data: List[Array[Double]],
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None,
    label: String = "query"
  ): HeatMap = {
    var idx = 0
    val timeseries = data.map { dps =>
      idx += 1
      TimeSeries(
        Map("series" -> idx.toString),
        label,
        new ArrayTimeSeq(DsType.Gauge, start, 60_000, dps)
      )
    }
    generateHeatmapSeries(timeseries, lowerBound, upperBound)
  }

  def generateHeatmapSeries(
    data: List[TimeSeries],
    lowerBound: Option[Double] = None,
    upperBound: Option[Double] = None
  ): HeatMap = {
    var min = Double.MaxValue
    var max = Double.MinValue
    var isPercentile = false
    var idx = 0
    val timeseries = data.map { ts =>
      if (isSpectatorPercentile(ts.tags)) {
        isPercentile = true
        val seconds = bktSeconds(LineDef(ts))
        if (seconds < min) min = seconds
        if (seconds > max) max = seconds
      } else {
        ts.data.foreach(start, end) { (_, dp) =>
          if (dp < min) min = dp
          if (dp > max) max = dp
        }
      }

      idx += 1
      LineDef(
        ts,
        lineStyle = LineStyle.HEATMAP
      )
    }

    val heatMapDef = HeatMapDef(
      lower = if (lowerBound.nonEmpty) Explicit(lowerBound.get) else AutoStyle,
      upper = if (upperBound.nonEmpty) Explicit(upperBound.get) else AutoStyle
    )
    val plotDef = generatePlotDef(timeseries, Some(heatMapDef), isPercentile)

    val graphDef = GraphDef(
      List(plotDef),
      Instant.ofEpochMilli(start),
      Instant.ofEpochMilli(end)
    )

    val yaxis =
      if (isPercentile) HeatMapTimerValueAxis(plotDef, graphDef.theme.axis, min, max, -1, -1)
      else LeftValueAxis(plotDef, graphDef.theme.axis, min, max)
    val timeAxis = TimeAxis(
      Style(color = graphDef.theme.axis.line.color),
      graphDef.startTime.toEpochMilli,
      graphDef.endTime.toEpochMilli,
      graphDef.step,
      graphDef.timezone
    )

    if (timeseries.filter(isSpectatorPercentile(_)).nonEmpty) {
      PercentileHeatMap(
        graphDef,
        plotDef,
        yaxis,
        timeAxis,
        x1 = 0,
        y1 = 0,
        x2 = 400,
        chartEnd = 200,
        -1,
        -1
      )
    } else {
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

  }

  def generatePlotDef(
    timeseries: List[LineDef],
    heatMapDef: Option[HeatMapDef] = None,
    isPercentile: Boolean = false
  ): PlotDef = {
    PlotDef(
      timeseries,
      scale = if (isPercentile) Scale.PERCENTILE else PlotDef(List.empty).scale,
      heatmapDef = heatMapDef
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
