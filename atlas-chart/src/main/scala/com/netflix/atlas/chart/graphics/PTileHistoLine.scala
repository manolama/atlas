package com.netflix.atlas.chart.graphics

import com.netflix.atlas.core.model.TimeSeq

import java.awt.Color
import java.awt.Graphics2D

case class PTileHistoLine(
  style: Style,
  ts: TimeSeq,
  xaxis: TimeAxis,
  yaxis: ValueAxis,
  countRange: Double,
  yOffset: Int,
  yHeight: Int
) extends Element {

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val step = ts.step
    val xscale = xaxis.scale(x1, x2)
    var t = xaxis.start
    // var pv = ts(t)
    while (t < xaxis.end) {
      val px1 = xscale(t - step)
      val px2 = xscale(t)

      val v = ts(t)
      val pct = v / countRange
      val alpha = (255 * pct).toInt
      Style(Color.RED).withAlpha(alpha).configure(g)

      g.fillRect(px1, yOffset, px2 - px1, yHeight)

      t += step
    }

  }
}
