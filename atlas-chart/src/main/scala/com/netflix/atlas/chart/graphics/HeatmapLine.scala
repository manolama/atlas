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
    ts.zip(ticks).foreach { tuple =>
      val (dp, tick) = tuple
      val px = xscale(tick.timestamp)
      if (dp > 0) {
        val c = palette.uniqueColors.reverse(colorScaler(dp))
        Style(c).configure(g)
        System.out.println(s" Y1 ${y1} Y2 ${y2}")
        g.fillRect(last, y1, px, y2)
      }
      last = px
    }
  }
}
