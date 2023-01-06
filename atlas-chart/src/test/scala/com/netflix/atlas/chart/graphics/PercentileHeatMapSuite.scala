package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.ptileScale
import com.netflix.atlas.chart.graphics.PercentileHeatMap.minMaxBuckets
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.spectator.api.histogram.PercentileBuckets
import munit.FunSuite

class PercentileHeatMapSuite extends FunSuite {

  val plotDef = PlotDef(List.empty, scale = Scale.PERCENTILE)
  val styles = Styles(Style(), Style(), Style())

  test("minMaxBuckets aligned normal range") {
    val min = 0.626349396 // 127 from bktSeconds
    val max = 2.863311529 // 137 from bktSeconds
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 126)
    assertEquals(maxBkt, 137)
  }

  test("minMaxBuckets unaligned normal range") {
    val min = 0.62634937
    val max = 2.863311501
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 126)
    assertEquals(maxBkt, 137)
  }

  test("minMaxBuckets unaligned normal range") {
    val min = 0.62634937
    val max = 2.863311501
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 126)
    assertEquals(maxBkt, 137)
  }

  test("minMaxBuckets unaligned single bucket") {
    val min = 0.62634938
    val max = 0.62634939
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 126)
    assertEquals(maxBkt, 127)
  }

  test("minMaxBuckets unaligned single value") {
    val min = 0.62634938
    val max = 0.62634938
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 126)
    assertEquals(maxBkt, 127)
  }

  test("minMaxBuckets 0 to next bucket") {
    val min = 0
    val max = 1.0 / 1000 / 1000 / 1000
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 0)
    assertEquals(maxBkt, 1)
  }

  test("minMaxBuckets 0 to 0") {
    val min = 0
    val max = 0.1 / 1000 / 1000 / 1000
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 0)
    assertEquals(maxBkt, 1)
  }

  test("minMaxBuckets bucket to max bucket") {
    val min = 4.2273788502251053E9
    val max = 9.3e9
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 274)
    assertEquals(maxBkt, 276)
  }

  test("minMaxBuckets max bucket to max bucket") {
    val min = 9.3e9
    val max = 9.3e12
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 275)
    assertEquals(maxBkt, 276)
  }

  test("minMaxBuckets negative values") {
    val min = -0.626349396
    val max = -2.863311529
    val (minBkt, maxBkt) = minMaxBuckets(min, max)
    assertEquals(minBkt, 0)
    assertEquals(maxBkt, 1)
  }

  test("scale") {
    val min = 1.9999999999999997e-9
    val max = 114.532461226
    val axis = HeatMapTimerValueAxis(plotDef, styles, min, max)
    val ticks = axis.ticks(5, 405)
    val scale = axis.scale(5, 405)
    val s = ptileScale(min, max, 5, 405)
    s.foreach { e =>
      System.out.println(
        s"Off: ${e.y} (${405 - e.y + 5}) Height: ${e.height}  Base: ${e.base}  BktIdx: ${e.bktIndex}"
      )
    }
    ticks.foreach { t =>
      System.out.println(s"Tick ${t.label} Y: ${scale(t.v)}")
    }
  }

  test("scale x2 buckets") {
    val min = 0.002796201
    val max = 0.002796201
    val axis = HeatMapTimerValueAxis(plotDef, styles, min, max)
    val ticks = axis.ticks(5, 305)
    val scale = axis.scale(5, 305)
    val s = ptileScale(min, max, 5, 305)

    val bi = 92
    System.out.println(s"Bkt IDX: ${bi}")
    System.out.println(s"Nanos: ${PercentileBuckets.get(bi)}")
    System.out.println(
      s"Idx for those nanos: ${PercentileBuckets.indexOf(PercentileBuckets.get(bi))}"
    )
    System.out.println(s"Idx for 126: ${PercentileBuckets.indexOf(126)}")
    System.out.println(s"Idx for 128: ${PercentileBuckets.indexOf(128)}")

    System.out.println("---------------")
    s.foreach { e =>
      System.out.println(
        s"Base: ${e.base} Off: ${e.y} (${scale(e.base)}) Height: ${e.height} to ${e.next} (${scale(e.next)})"
      )
    }
    System.out.println("----------------")
    ticks.foreach { t =>
      System.out.println(s"Tick ${t.label} Y : ${scale(t.v)} Val: ${t.v} Maj: ${t.major} ")
    }
  }

  test("scale x buckets") {
    val min = 51.53960755
    val max = 114.532461226
    val axis = HeatMapTimerValueAxis(plotDef, styles, min, max)
    val ticks = axis.ticks(5, 305)
    val scale = axis.scale(5, 305)
    val s = ptileScale(min, max, 5, 305)
    s.foreach { e =>
      System.out.println(
        s"Off: ${e.y} (${scale(e.base)}) Height: ${e.height}  Base: ${e.base} "
      )
    }
    ticks.foreach { t =>
      System.out.println(s"Tick ${t.label} Y: ${scale(t.v)}")
    }
  }

  test("scale 1 bucket") {
    val s = ptileScale(51.53960755, 51.53960755, 5, 405)
    s.foreach { e =>
      System.out.println(
        s"Off: ${e.y} (${405 - e.y + 5}) Height: ${e.height}  Base: ${e.base} "
      )
    }
  }

  test("Foo") {
    System.out.println(PercentileBuckets.asArray().length)
    System.out.println(bktSeconds(274))
    System.out.println(bktSeconds(275))

  }
}
