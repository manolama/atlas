package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.Heatmap.singleColorAlphas
import com.netflix.atlas.chart.graphics.HeatmapSuite.assertColorMap
import com.netflix.atlas.chart.graphics.HeatmapSuite.assertRowCounts
import com.netflix.atlas.chart.graphics.HeatmapSuite.generateHeatmap
import com.netflix.atlas.chart.graphics.HeatmapSuite.generateHeatmapSeries
import com.netflix.atlas.core.index.DataSet.wave
import munit.FunSuite

import java.time.Duration
import java.util

class BasicHeatmapSuite extends FunSuite {

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
    assertEquals(heatmap.legendLabel, "query")
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

  test("explicit bounds") {
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
    assertEquals(heatmap.legendLabel, "My Heatmap")
  }

}
