package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.Heatmap.singleColorAlphas
import com.netflix.atlas.chart.graphics.HeatmapSuite.assertColorMap
import com.netflix.atlas.chart.graphics.HeatmapSuite.assertRowCounts
import com.netflix.atlas.chart.graphics.HeatmapSuite.generateHeatmapSeries
import com.netflix.atlas.chart.graphics.HeatmapSuite.start
import com.netflix.atlas.chart.graphics.PercentileHeatmap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatmap.getPtileScale
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite
import org.junit.Assert.assertFalse

import java.util

class PercentileHeatmapSuite extends FunSuite {

  val plotDef = PlotDef(List.empty, scale = Scale.PERCENTILE)
  val styles = Styles(Style(), Style(), Style())

  test("getPtileScale min/max aligned range no buckets") {
    val min = 0.626349396 // 127 from bktSeconds
    val max = 2.863311529 // 137 from bktSeconds
    validate127to137(getPtileScale(min, max, 5, 305, -1, -1))
  }

  test("getPtileScale min/max aligned range with buckets") {
    val min = 0.626349396 // 127 from bktSeconds
    val max = 2.863311529 // 137 from bktSeconds
    validate127to137(getPtileScale(min, max, 5, 305, 127, 137))
  }

  test("getPtileScale unaligned range no buckets") {
    val min = 0.62635 // 127 from bktSeconds
    val max = 2.864 // 137 from bktSeconds
    validate127to137(getPtileScale(min, max, 5, 305, -1, -1))
  }

  test("getPtileScale unaligned range with buckets") {
    val min = 0.62635 // 127 from bktSeconds
    val max = 2.864 // 137 from bktSeconds
    validate127to137(getPtileScale(min, max, 5, 305, 127, 137))
  }

  test("getPtileScale single bucket") {
    val min = 0.626349396
    val buckets = getPtileScale(min, min, 5, 305, -1, -1)
    assertEquals(buckets.size, 2)
    var bkt = buckets(0)
    assertEquals(bkt.baseDuration, bktSeconds(127))
    assertEquals(bkt.nextDuration, bktSeconds(128))
    assertFalse(bkt.skipTick)
    assert(bkt.majorTick)
    assertEquals(bkt.subTicks.size, 29)

    bkt = buckets(1)
    assertEquals(bkt.baseDuration, bktSeconds(128))
    assertEquals(bkt.nextDuration, bktSeconds(129))
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
    assertEquals(heatmap.rows.size, 19)
    for (i <- 0 until heatmap.rows.size) {
      assertRowCounts(0.10, 0.05, heatmap.rows(i))
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
    assertEquals(heatmap.rows.size, 18)
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
    assertEquals(heatmap.rows.size, 19)
    assertRowCounts(6, 3, heatmap.rows(0))
    for (i <- 1 until heatmap.rows.size) {
      assertRowCounts(8, 4, heatmap.rows(i))
    }
    assertEquals(heatmap.yticks.head.label, "49.2μs")
    assertEquals(heatmap.yticks.last.label, "4.3s")

    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas(5), 3, 3),
        (singleColorAlphas(4), 4, 4),
        (singleColorAlphas(2), 6, 6),
        (singleColorAlphas.head, 8, 8)
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
    assertEquals(heatmap.rows.size, 19)
    assertRowCounts(6, 0, heatmap.rows(0))
    for (i <- 1 until heatmap.rows.size) {
      assertRowCounts(8, 4, heatmap.rows(i))
    }
    assertEquals(heatmap.yticks.head.label, "49.2μs")
    assertEquals(heatmap.yticks.last.label, "4.3s")

    assertColorMap(
      heatmap.colorMap,
      List(
        (singleColorAlphas(6), 4, 4),
        (singleColorAlphas(4), 6, 6),
        (singleColorAlphas(1), 8, 8)
      )
    )
    assertEquals(heatmap.legendLabel, "query")
  }

  def validate127to137(buckets: List[PtileScale]): Unit = {
    assertEquals(buckets.size, 12)

    var bktIdx = 127
    buckets.foreach { bkt =>
      assertEquals(bkt.baseDuration, bktSeconds(bktIdx))
      assertEquals(bkt.nextDuration, bktSeconds(bktIdx + 1))
      assertFalse(bkt.skipTick)
      assert(bkt.majorTick)
      if (bktIdx == 138) assertEquals(bkt.subTicks.size, 0)
      else assertEquals(bkt.subTicks.size, 1)
      bktIdx += 1
    }
  }
}
