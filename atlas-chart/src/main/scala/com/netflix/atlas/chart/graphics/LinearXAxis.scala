package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.Scale

import java.awt.Graphics2D

case class LinearXAxis(
  style: Style,
  start: Long,
  end: Long,
  step: Long,
  alpha: Int = 40
) extends XAxis {

  override def height: Int = 10 + ChartSettings.smallFontDims.height

  def scale(p1: Int, p2: Int): Scales.LongScale = {
    Scales.time(start - step, end - step, step, p1, p2)
  }

  def ticks(x1: Int, x2: Int): List[LongTick] = {

    // The first interval displayed will end at the start time. For calculating ticks the
    // start time is adjusted so we can see minor ticks within the first interval
    val numTicks = (x2 - x1) / LinearXAxis.minTickLabelWidth
//    Ticks.time(start - step, end, zone, numTicks, runMode)
    val s = if (start < 0) 0 else start - step
    val n = ((end - s) / step).toInt
    Ticks
      .value(s.toDouble, end.toDouble, n, Scale.LINEAR)
      .map(t => LongTick(t.v.toInt, if (t.v >= 0) t.major else false))
  }

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val txtH = ChartSettings.smallFontDims.height
    val labelPadding = LinearXAxis.minTickLabelWidth / 2

    // Horizontal line across the bottom of the chart. The main horizontal line for the axis is
    // made faint so it is easier to see lines in the chart that are directly against the axis.
    style.withAlpha(alpha).configure(g)
    g.drawLine(x1, y1, x2, y1)

    style.configure(g)
    val xscale = scale(x1, x2)
    val majorTicks = ticks(x1, x2).filter(_.major)
    majorTicks.foreach { tick =>
      val px = xscale(tick.value)
      if (px >= x1 && px <= x2) {
        // Vertical tick mark
        g.drawLine(px, y1, px, y1 + 4)

        // Label for the tick mark
        val txt = Text(tick.label, font = ChartSettings.smallFont, style = style)
        txt.draw(g, px - labelPadding, y1 + txtH / 2, px + labelPadding, y1 + txtH)
      }
    }
  }
}

object LinearXAxis {
  private val minTickLabelWidth = " 12345 ".length * ChartSettings.smallFontDims.width
}
