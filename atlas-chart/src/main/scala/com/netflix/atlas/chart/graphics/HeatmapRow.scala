package com.netflix.atlas.chart.graphics

import java.awt.Graphics2D

case class HeatmapRow(
  ts: Array[Double],
  xaxis: TimeAxis,
  heatmap: HeatMap
) extends Element {

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val ticks = xaxis.ticks(x1, x2)
    val xscale = xaxis.scale(x1, x2)
    val ti = ticks.iterator
    var last = x1
    ts.foreach { dp =>
      val x = if (ti.hasNext) xscale(ti.next().timestamp) else x2
      if (dp > 0) {
        val c = heatmap.getColor(dp)
        Style(c).configure(g)
        g.fillRect(last, y1, x - last, y2)
      }
      last = x
    }
  }
}
