package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.HeatMap.choosePalette
import com.netflix.atlas.chart.graphics.HeatMap.colorScaler
import com.netflix.atlas.chart.graphics.HeatMap.singleColorAlphas
import com.netflix.atlas.chart.graphics.Scales.DoubleScale
import com.netflix.atlas.chart.model.HeatmapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Strings
import munit.FunSuite

import java.awt.Color
import java.awt.Graphics2D

class HeatMapSuite extends FunSuite {

  private val ts = TimeSeries(
    Map.empty,
    new ArrayTimeSeq(DsType.Gauge, 1672819200000L, 60_000, new Array[Double](59))
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
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatmapDef(scale = Scale.LOGARITHMIC)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    val expected = Array(0, 1, 3, 4, 5, 5, 6)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), expected(i - 1))
    }
  }

  test("colorScaler power 2") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatmapDef(scale = Scale.POWER_2)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    val expected = Array(0, 0, 0, 1, 2, 3, 5)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), expected(i - 1))
    }
  }

  test("colorScaler square root") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatmapDef(scale = Scale.SQRT)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), i - 1)
    }
  }

  test("colorScaler percentile switches to log") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatmapDef(scale = Scale.PERCENTILE)))
    val scaler = colorScaler(plotDef, palette, 1, palette.uniqueColors.length)
    val expected = Array(0, 1, 3, 4, 5, 5, 6)
    for (i <- 1 to palette.uniqueColors.length) {
      assertEquals(scaler(i), expected(i - 1))
    }
  }

  test("colorScaler small values to linear") {
    val plotDef = PlotDef(List.empty, heatmapDef = Some(HeatmapDef(scale = Scale.LOGARITHMIC)))
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
