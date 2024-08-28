package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.Scales.LongScale

import java.awt.Color
import java.awt.Graphics2D
import java.time.Duration
import java.time.ZoneId

case class GenericAxis(
  style: Style,
  labels: List[String],
  alpha: Int = 40
) extends XAxis {

  override def height: Int = 10 + ChartSettings.smallFontDims.height

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val txtH = ChartSettings.smallFontDims.height
    val labelPadding = GenericAxis.minTickLabelWidth / 2

    // Horizontal line across the bottom of the chart. The main horizontal line for the axis is
    // made faint so it is easier to see lines in the chart that are directly against the axis.
    style.withAlpha(alpha).configure(g)
    g.drawLine(x1, y1, x2, y1)

    style.configure(g)
    val xscale = scale(x1, x2)

    // TODO - proper ticks
    val pixelsPerStep = (x2 - x1) / labels.size
    for (i <- 0 until labels.size) {
      val px = xscale(i)
      if (px >= x1 && px <= x2) {
        // Vertical tick mark
        g.drawLine(px, y1, px, y1 + 4)

        // TODO - truncation, etc.
        val txt = Text(labels(i), font = ChartSettings.smallFont, style = style)
        txt.draw(g, px - labelPadding, y1 + txtH / 2, px + labelPadding, y1 + txtH)

      }
    }
  }

  override def ticks(x1: Int, x2: Int): List[TimeTick] = {
    // TODO - temp, do some real ticks
    val ticks = for (idx <- 0 until labels.size) yield {
      TimeTick(idx, ZoneId.of("UTC"))
    }
    ticks.toList
  }

  override def scale(p1: Int, p2: Int): LongScale = { idx =>
    val pixelsPerStep = (p2 - p1) / labels.size
    val ret = p1 + ((idx.toInt + 1) * pixelsPerStep)
    ret
  }

  override def start: Long = 0

  override def end: Long = labels.size

  override def step: Long = 1

  override val size: Int = labels.size
}

object GenericAxis {
  private val minTickLabelWidth = " 00:00 ".length * ChartSettings.smallFontDims.width
}
