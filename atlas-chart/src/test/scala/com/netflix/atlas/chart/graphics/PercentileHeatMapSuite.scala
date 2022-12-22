package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.getScale
import com.netflix.atlas.chart.graphics.PercentileHeatMap.minMaxBuckets
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.spectator.api.histogram.PercentileBuckets
import munit.FunSuite

class PercentileHeatMapSuite extends FunSuite {

  val plotDef = PlotDef(List.empty, scale = Scale.PERCENTILE)
  val styles = Styles(Style(), Style(), Style())

  test("scale") {
    val min = 1.9999999999999997e-9
    val max = 114.532461226
    val axis = HeatMapTimerValueAxis(plotDef, styles, min, max)
    val ticks = axis.ticks(5, 405)
    val scale = axis.scale(5, 405)
    val s = getScale(min, max, 5, 405)
    s.foreach { e =>
      System.out.println(
        s"Off: ${e.y} (${405 - e.y + 5}) Height: ${e.height}  Base: ${e.base}  BktIdx: ${e.bktIndex}"
      )
    }
    ticks.foreach { t =>
      System.out.println(s"Tick ${t.label} Y: ${scale(t.v)}")
    }
  }

  test("Bkts Brigade") {
    val bkts = PercentileBuckets.asArray()
    bkts.take(16).zipWithIndex.foreach { tuple =>
      val (v, arrayIdx) = tuple
      val bktIdx = PercentileBuckets.indexOf(v)
      val got = PercentileBuckets.get(bktIdx)
      System.out.println(s"Array ${v}@${arrayIdx} ")
    }

    // pick
    System.out.println("-----------")
    System.out.println(
      s"Bkt for val 14 -> ${PercentileBuckets.indexOf(14)} w v ${PercentileBuckets.get(PercentileBuckets.indexOf(14))}"
    )
    System.out.println(
      s"Bkt for val 15 -> ${PercentileBuckets.indexOf(15)} w v ${PercentileBuckets.get(PercentileBuckets.indexOf(15))}"
    )
    System.out.println(
      s"Bkt for val 16 -> ${PercentileBuckets.indexOf(16)} w v ${PercentileBuckets.get(PercentileBuckets.indexOf(16))}"
    )
    System.out.println(
      s"Bkt for val 20 -> ${PercentileBuckets.indexOf(20)} w v ${PercentileBuckets.get(PercentileBuckets.indexOf(20))}"
    )
    System.out.println(
      s"Bkt for val 21 -> ${PercentileBuckets.indexOf(21)} w v ${PercentileBuckets.get(PercentileBuckets.indexOf(21))}"
    )
  }

  test("scale x2 buckets") {
    val min = 0.002796201
    val max = 0.002796201
    val axis = HeatMapTimerValueAxis(plotDef, styles, min, max)
    val ticks = axis.ticks(5, 305)
    val scale = axis.scale(5, 305)
    val s = getScale(min, max, 5, 305)

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
    val s = getScale(min, max, 5, 305)
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
    val s = getScale(51.53960755, 51.53960755, 5, 405)
    s.foreach { e =>
      System.out.println(
        s"Off: ${e.y} (${405 - e.y + 5}) Height: ${e.height}  Base: ${e.base} "
      )
    }
  }
}
