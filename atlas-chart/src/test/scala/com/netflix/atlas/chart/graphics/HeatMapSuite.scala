package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.HeatMap.choosePalette
import com.netflix.atlas.chart.graphics.HeatMap.singleColorAlphas
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Strings
import munit.FunSuite

import java.awt.Color

class HeatMapSuite extends FunSuite {

  private val ts = TimeSeries(
    Map.empty,
    new ArrayTimeSeq(DsType.Gauge, 1672819200000L, 60_000, new Array[Double](59))
  )

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
}
