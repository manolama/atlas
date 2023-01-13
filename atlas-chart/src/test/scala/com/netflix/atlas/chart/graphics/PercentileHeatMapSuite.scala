package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.HeatMap.singleColorAlphas
import com.netflix.atlas.chart.graphics.HeatMapSuite.assertColorMap
import com.netflix.atlas.chart.graphics.HeatMapSuite.assertRowCounts
import com.netflix.atlas.chart.graphics.HeatMapSuite.generateHeatmapSeries
import com.netflix.atlas.chart.graphics.HeatMapSuite.start
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktNanos
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.getPtileScale
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite
import org.junit.Assert.assertFalse

import java.util

class PercentileHeatMapSuite extends FunSuite {

  val plotDef = PlotDef(List.empty, scale = Scale.PERCENTILE)
  val styles = Styles(Style(), Style(), Style())

  test("getPtileScale aligned normal range") {
    val min = 0.626349396 // 127 from bktSeconds
    val max = 2.863311529 // 137 from bktSeconds
    val buckets = getPtileScale(min, max, 5, 305, -1, -1)
    assertEquals(buckets.size, 11)

    var bktIdx = 125
    buckets.foreach { bkt =>
      assertEquals(bkt.baseDuration, bktSeconds(bktIdx))
      assertEquals(bkt.nextDuration, bktSeconds(bktIdx + 1))
      assertFalse(bkt.skipTick)
      assert(bkt.majorTick)
      assert(bkt.subTicks.isEmpty)
      bktIdx += 1
    }
    System.out.println(buckets)
  }

  test("1 bucket") {
    val dps = new Array[Double](60)
    util.Arrays.fill(dps, 1)
    val ts = TimeSeries(
      Map("percentile" -> "T0042"),
      "query",
      new ArrayTimeSeq(DsType.Gauge, start, 60_000, dps)
    )
    val heatmap = generateHeatmapSeries(List(ts))
    assertEquals(heatmap.rows.size, 20)
    for (i <- 0 until heatmap.rows.size) {
      assertRowCounts(0.1, 0.05, heatmap.rows(i))
    }
    assertEquals(heatmap.yticks.head.label, "49.2μs")
    assertEquals(heatmap.yticks.last.label, "54.6μs")

    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 0.05, 0.05),
        (singleColorAlphas(0), 0.1, 0.1)
      )
    )
    assertEquals(heatmap.legendLabel, "query")
  }

  test("2 buckets") {
    val dps = new Array[Double](60)
    util.Arrays.fill(dps, 1)
    val ts1 = TimeSeries(
      Map("percentile" -> "T0042"),
      "query",
      new ArrayTimeSeq(DsType.Gauge, start, 60_000, dps)
    )
    val ts2 = TimeSeries(
      Map("percentile" -> "T0043"),
      "query",
      new ArrayTimeSeq(DsType.Gauge, start, 60_000, dps)
    )
    val heatmap = generateHeatmapSeries(List(ts1, ts2))
    assertEquals(heatmap.rows.size, 20)
    for (i <- 0 until heatmap.rows.size) {
      assertRowCounts(0.2, 0.1, heatmap.rows(i))
    }
    assertEquals(heatmap.yticks.head.label, "49.2μs")
    assertEquals(heatmap.yticks.last.label, "60.1μs")

    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas.last, 0.1, 0.1),
        (singleColorAlphas(0), 0.2, 0.2)
      )
    )
    assertEquals(heatmap.legendLabel, "query")
  }

  test("more buckets than ticks") {
    val dps = new Array[Double](60)
    util.Arrays.fill(dps, 1)
    val series = List.newBuilder[TimeSeries]
    var bkt = Integer.parseInt("0042", 16)
    for (_ <- 0 until 75) {
      series += TimeSeries(
        Map("percentile" -> s"T00${bkt.toHexString.toUpperCase()}"),
        "query",
        new ArrayTimeSeq(DsType.Gauge, start, 60_000, dps)
      )
      bkt += 1
    }
    val heatmap = generateHeatmapSeries(series.result())
    assertEquals(heatmap.rows.size, 25)
    for (i <- 0 until heatmap.rows.size) {
      assertRowCounts(6, 3, heatmap.rows(i))
    }
    assertEquals(heatmap.yticks.head.label, "49.2μs")
    assertEquals(heatmap.yticks.last.label, "4.3s")

    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas(4), 3, 3),
        (singleColorAlphas(1), 6, 6)
      )
    )
    assertEquals(heatmap.legendLabel, "query")
  }

  test("explicit bounds") {
    val dps = new Array[Double](60)
    util.Arrays.fill(dps, 1)
    val series = List.newBuilder[TimeSeries]
    var bkt = Integer.parseInt("0042", 16)
    for (_ <- 0 until 75) {
      series += TimeSeries(
        Map("percentile" -> s"T00${bkt.toHexString.toUpperCase()}"),
        "query",
        new ArrayTimeSeq(DsType.Gauge, start, 60_000, dps)
      )
      bkt += 1
    }
    val heatmap = generateHeatmapSeries(series.result(), Some(4))
    assertEquals(heatmap.rows.size, 25)
    for (i <- 0 until heatmap.rows.size) {
      assertRowCounts(6, 0, heatmap.rows(i))
    }
    assertEquals(heatmap.yticks.head.label, "49.2μs")
    assertEquals(heatmap.yticks.last.label, "4.3s")

    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas(2), 6, 6)
      )
    )
    assertEquals(heatmap.legendLabel, "query")
  }

}
