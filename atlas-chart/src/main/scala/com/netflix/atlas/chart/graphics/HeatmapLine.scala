package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.core.model.TimeSeq

import java.awt.Graphics2D

case class HeatmapLine(
  ts: Array[Long],
  xaxis: TimeAxis,
  colorScaler: Scales.DoubleScale,
  palette: Palette
) extends Element {

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val ticks = xaxis.ticks(x1, x2)
    val xscale = xaxis.scale(x1, x2)
    var last = x1
    val ti = ticks.iterator
    ts.foreach { dp =>
      val x = if (ti.hasNext) xscale(ti.next().timestamp) else x2
      if (dp > 0) {
        val c = palette.uniqueColors(colorScaler(dp))
        Style(c).configure(g)
        System.out.println(s" last ${last} px ${x}  Y1 ${y1} height ${y2}")
        g.fillRect(last, y1, x - last, y2)
      }
      last = x
    }
//    ts.zip(ticks).foreach { tuple =>
//      val (dp, tick) = tuple
//      val px = xscale(tick.timestamp)
//      if (dp > 0) {
//        val c = palette.uniqueColors(colorScaler(dp))
//        Style(c).configure(g)
//        System.out.println(s" last ${last} px ${px}  Y1 ${y1} height ${y2}")
//        g.fillRect(last, y1, px - last, y2)
//      }
//      last = px
//    }
  }
}
