package com.netflix.atlas.chart.graphics

import com.netflix.atlas.core.model.TimeSeq

import java.awt.Color
import java.awt.Graphics2D

case class PTileHistoLine(
  style: Style,
  ts: TimeSeq,
  xaxis: TimeAxis,
  yaxis: ValueAxis,
  countRange: Long,
  yOffset: Int,
  yHeight: Int,
  colorScaler: Scales.DoubleScale,
  colorList: List[Color]
) extends Element {

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val step = ts.step
    val xscale = xaxis.scale(x1, x2)
    var t = xaxis.start
    // var pv = ts(t)
    // System.out.print(s"Y: ${yOffset} => ")
    while (t < xaxis.end) {
      val px1 = xscale(t - step)
      val px2 = xscale(t)

      val v = ts(t).toLong
      if (v > 0) {
        if (yOffset != 403) {
          val c = colorList.reverse(colorScaler(v))
          Style(c).configure(g)
        } else {
          val pct = v.toDouble / countRange.toDouble
          // System.out.print(s"${v}, ")
          val alpha = 25 + (230 * pct).toInt
          // System.out.print("(%d, %.0f, %d), ".format(v, pct * 100, alpha))
          // System.out.print(s"${alpha}, ")
          Style(Color.BLUE).configure(g)
        }
        g.fillRect(px1, yOffset, px2 - px1, yHeight)
      }
      t += step
    }
    // System.out.println()

  }
}
