package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.PercentileHeatMap.getScale
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
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
        s"Off: ${e.offset} (${405 - e.offset + 5}) Height: ${e.height}  Boundary: ${e.boundary}  BktIdx: ${e.bktIndex}"
      )
    }
    ticks.foreach { t =>
      System.out.println(s"Tick ${t.label} Y: ${scale(t.v)}")
    }
  }

  test("scale 2 buckets") {
    val min = 51.53960755
    val max = 57.266230611
    val axis = HeatMapTimerValueAxis(plotDef, styles, min, max)
    val ticks = axis.ticks(5, 405)
    val scale = axis.scale(5, 405)
    val s = getScale(min, max, 5, 405)
    s.foreach { e =>
      System.out.println(
        s"Off: ${e.offset} (${405 - e.offset + 5}) Height: ${e.height}  Boundary: ${e.boundary} split? ${e.split}"
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
        s"Off: ${e.offset} (${405 - e.offset + 5}) Height: ${e.height}  Boundary: ${e.boundary} split? ${e.split}"
      )
    }
  }
}
