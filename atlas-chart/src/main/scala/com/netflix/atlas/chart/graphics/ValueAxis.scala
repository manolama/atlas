/*
 * Copyright 2014-2022 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.chart.graphics

import java.awt.Graphics2D
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.TickLabelMode
import com.netflix.atlas.core.util.UnitPrefix
import com.netflix.spectator.api.histogram.PercentileBuckets

sealed trait ValueAxis extends Element with FixedWidth {

  import ValueAxis._

  override def width: Int = labelHeight + tickLabelWidth + tickMarkLength + 1

  def plotDef: PlotDef

  def min: Double

  def max: Double

  def styles: Styles

  val style: Style = {
    val axisColor = plotDef.getAxisColor(styles.line.color)
    styles.line.copy(color = axisColor)
  }

  val label: Option[Text] = plotDef.ylabel.map { str =>
    Text(str, style = style)
  }

  val valueScale: Scales.DoubleFactory = Scales.factory(plotDef.scale)

  def scale(y1: Int, y2: Int): Scales.DoubleScale = valueScale(min, max, y1, y2)

  def ticks(y1: Int, y2: Int): List[ValueTick] = {
    val numTicks = (y2 - y1) / minTickLabelHeight
    plotDef.tickLabelMode match {
      case TickLabelMode.BINARY   => Ticks.binary(min, max, numTicks)
      case TickLabelMode.DURATION => Ticks.duration(min, max, numTicks)
      case _                      => Ticks.value(min, max, numTicks, plotDef.scale)
    }
  }

  protected def angle: Double

  protected def drawLabel(text: Text, g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val transform = g.getTransform
    val centerX = (x2 - x1) / 2 + x1
    val centerY = (y2 - y1) / 2 + y1

    val width = y2 - y1
    val truncated = text.truncate(width)
    val height = truncated.computeHeight(g, width)
    g.rotate(angle, centerX, centerY)
    truncated.draw(
      g,
      centerX - width / 2,
      centerY - height / 2,
      centerX + width / 2,
      centerY + height / 2
    )
    g.setTransform(transform)
  }

  protected def tickPrefix(v: Double): UnitPrefix = {
    plotDef.tickLabelMode match {
      case TickLabelMode.OFF      => UnitPrefix.one
      case TickLabelMode.DECIMAL  => UnitPrefix.decimal(v)
      case TickLabelMode.BINARY   => UnitPrefix.binary(v)
      case TickLabelMode.DURATION => UnitPrefix.duration(v)
    }
  }

  protected def tickLabelFmt: String = {
    plotDef.tickLabelMode match {
      case TickLabelMode.OFF      => ""
      case TickLabelMode.DECIMAL  => "%.1f%s"
      case TickLabelMode.BINARY   => "%.0f%s"
      case TickLabelMode.DURATION => "%.1f%s"
    }
  }
}

case class LeftValueAxis(plotDef: PlotDef, styles: Styles, min: Double, max: Double)
    extends ValueAxis {

  import ValueAxis._

  protected def angle: Double = -Math.PI / 2.0

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    g.drawLine(x2, y1, x2, y2)

    val majorTicks = ticks(y1, y2).filter(_.major)
    val offset = if (majorTicks.isEmpty) 0.0 else majorTicks.head.offset
    if (offset == 0.0 || !plotDef.showTickLabels) {
      drawNormal(majorTicks, g, x1, y1, x2, y2)
    } else {
      drawWithOffset(majorTicks, g, x1, y1, x2, y2)
    }

    label.foreach { t =>
      drawLabel(t, g, x1, y1, x1 + labelHeight, y2)
    }
  }

  // FOR PTILE HEAT
  override def ticks(y1: Int, y2: Int): List[ValueTick] = {
    val numTicks = (y2 - y1) / minTickLabelHeight
    plotDef.tickLabelMode match {
      case TickLabelMode.BINARY   => Ticks.binary(min, max, numTicks)
      case TickLabelMode.DURATION => Ticks.duration(min, max, numTicks)
      case _                      => Ticks.value(min, max, numTicks, plotDef.scale)
    }
  }

  private def drawNormal(
    ticks: List[ValueTick],
    g: Graphics2D,
    x1: Int,
    y1: Int,
    x2: Int,
    y2: Int
  ): Unit = {
    val yscale = scale(y1, y2)
    ticks.foreach { tick =>
      val py = yscale(tick.v)
      g.drawLine(x2, py, x2 - tickMarkLength, py)

      if (plotDef.showTickLabels) {
        val txt = Text(
          tick.label,
          font = ChartSettings.smallFont,
          alignment = TextAlignment.RIGHT,
          style = style
        )
        val txtH = ChartSettings.smallFontDims.height
        val ty = py - txtH / 2
        txt.draw(g, x1, ty, x2 - tickMarkLength - 1, ty + txtH)
      }
    }
  }

  private def drawWithOffset(
    ticks: List[ValueTick],
    g: Graphics2D,
    x1: Int,
    y1: Int,
    x2: Int,
    y2: Int
  ): Unit = {
    val offset = ticks.head.v
    val prefix = tickPrefix(ticks.last.v - offset)
    val offsetStr = prefix.format(offset, tickLabelFmt)
    val offsetTxt =
      Text(offsetStr, font = ChartSettings.smallFont, alignment = TextAlignment.LEFT, style = style)
    val offsetH = ChartSettings.smallFontDims.height * 2
    val offsetW = ChartSettings.smallFontDims.width * (offsetStr.length + 3)

    val yscale = scale(y1, y2)
    val oy = yscale(offset)
    val otop = oy - offsetW - tickMarkLength
    val obottom = oy - tickMarkLength
    drawLabel(offsetTxt, g, x2 - offsetH - tickMarkLength, otop, x2 - tickMarkLength, obottom)
    g.drawLine(x2, oy, x2 - offsetH - tickMarkLength, oy)

    ticks.tail.foreach { tick =>
      val py = yscale(tick.v)
      g.drawLine(x2, py, x2 - tickMarkLength, py)

      if (plotDef.showTickLabels) {
        val label = s"+${prefix.format(tick.v - offset, tickLabelFmt)}"
        val txt =
          Text(
            label,
            font = ChartSettings.smallFont,
            alignment = TextAlignment.RIGHT,
            style = style
          )
        val txtH = ChartSettings.smallFontDims.height
        val ty = py - txtH / 2
        if (ty + txtH < otop)
          txt.draw(g, x1, ty, x2 - tickMarkLength - 1, ty + txtH)
      }
    }
  }
}

case class HeatMapTimerValueAxis(plotDef: PlotDef, styles: Styles, min: Double, max: Double)
    extends ValueAxis {

  import ValueAxis._

  protected var skipBuckets = 0

  protected def angle: Double = -Math.PI / 2.0

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    g.drawLine(x2, y1, x2, y2)

    val ticks = getTicks(y1, y2)
    drawNormal(ticks, g, x1, y1, x2, y2)

    label.foreach { t =>
      drawLabel(t, g, x1, y1, x1 + labelHeight, y2)
    }
  }

  // FOR PTILE HEAT
  def getTicks(y1: Int, y2: Int): Map[Int, ValueTick] = {
    val numTicks = (y2 - y1) / minTickLabelHeight
    val ticks = Map.newBuilder[Int, ValueTick]
    val bktRange = max - min
    skipBuckets = (bktRange / numTicks / 4).toInt
    System.out.println(s"Getting ticks.... R: ${bktRange}  Skip ${skipBuckets}")
    var cnt = 0
    for (i <- min.toInt to max.toInt) {
      if (i % skipBuckets == 0) {
        val sec = bktSeconds(i)
        val prefix = Ticks.getDurationPrefix(sec, sec)
        // val fmt = Ticks.durationLabelFormat(prefix, sec)
        val fmt = prefix.format(sec, "%.1f%s")
        val label = prefix.format(sec, fmt)
        val t = ValueTick(i, 0.0, i % numTicks == 0, Some(label))
        //      System.out.println(s"  [${i}]    ${t}")
        ticks += i -> t
        cnt += 1
      }
    }

//    val sec = bktSeconds(max.toInt)
//    val prefix = Ticks.getDurationPrefix(sec, sec)
//    val fmt = Ticks.durationLabelFormat(prefix, sec)
//    val label = prefix.format(sec, fmt)
//    val t = ValueTick(max.toInt, 0.0, true, Some(label))
//    System.out.println(s"  [${max.toInt}]    ${t}")
//    ticks += t
    ticks.result()
  }

  def bktSeconds(index: Int): Double = {
    PercentileBuckets.get(index).toDouble / 1000 / 1000 / 1000
  }

  private def drawNormal(
    ticks: Map[Int, ValueTick],
    g: Graphics2D,
    x1: Int,
    y1: Int,
    x2: Int,
    y2: Int
  ): Unit = {
    // val yscale = scale(y1, y2)

    // y2 is chart end

    // tmep
    val bktRange = (max.toInt - min.toInt) + 1
    val dpHeight = ((y2 + 1) - y1) / bktRange.toDouble
    // val yFudge = Math.round(bktRange.toDouble / ((y2 - y1) - (bktRange * dpHeight)))
    var ctr = 0
    var previousOffset = y2.toDouble + 1
    for (i <- min.toInt to max.toInt) {
      // val py = yscale(tick.v)
      // val py = y2 - (tick.v.toInt * 3)
      val nextOffset = previousOffset - dpHeight
      val h = (Math.round(previousOffset) - Math.round(nextOffset)).toInt
      val offset = Math.round(previousOffset).toInt - h
      previousOffset = nextOffset

      ticks.get(i) match {
        case Some(tick) =>
          if (tick.major && offset >= y1) {
            // g.drawLine(x2, py, x2 - tickMarkLength, py)
            g.drawLine(x2, offset, x2 - tickMarkLength, offset)

            if (plotDef.showTickLabels) {
              val txt = Text(
                tick.label,
                font = ChartSettings.smallFont,
                alignment = TextAlignment.RIGHT,
                style = style
              )
              val txtH = ChartSettings.smallFontDims.height
              // val ty = py - txtH / 2
              val ty = offset - txtH / 2
              txt.draw(g, x1, ty, x2 - tickMarkLength - 1, ty + txtH)
            }
            System.out.println(
              s" ***** tick Height ${h} @ ${tick.v.toInt}  Offset ${offset} -> ${tick.label}"
            )
          } else {
            System.out.println(
              s"       tick Height ${h} @ ${tick.v.toInt}  Offset ${offset} -> ${tick.label}"
            )
          }
        case None => // no-op
      }

      // offset -= offsetAdjust(ctr, yFudge, dpHeight)
      // overshoots it!!!!!!!! e.g. 1.9m is at offset 27 instead of 19 as it should be
//      for (_ <- 0 until skipBuckets - 1) {
//        previousOffset -= dpHeight
//      }
      ctr += 1 + skipBuckets
//      ctr += 1
//      offset -= h
    }
  }

  def offsetAdjust(idx: Int, yFudge: Long, dpHeight: Int): Int = {
    var ctr = idx
    var offset = 0
    for (_ <- 0 until skipBuckets) {
      val h = if (ctr % yFudge == 0) dpHeight + 1 else dpHeight
      offset += h
      ctr += 1
    }
    offset
  }
}

case class RightValueAxis(plotDef: PlotDef, styles: Styles, min: Double, max: Double)
    extends ValueAxis {

  import ValueAxis._

  protected def angle: Double = Math.PI / 2.0

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    g.drawLine(x1, y1, x1, y2)

    val majorTicks = ticks(y1, y2).filter(_.major)
    val offset = if (majorTicks.isEmpty) 0.0 else majorTicks.head.offset
    if (offset == 0.0 || !plotDef.showTickLabels) {
      drawNormal(majorTicks, g, x1, y1, x2, y2)
    } else {
      drawWithOffset(majorTicks, g, x1, y1, x2, y2)
    }

    label.foreach { t =>
      drawLabel(t, g, x2 - labelHeight, y1, x2, y2)
    }
  }

  def drawNormal(
    ticks: List[ValueTick],
    g: Graphics2D,
    x1: Int,
    y1: Int,
    x2: Int,
    y2: Int
  ): Unit = {
    val yscale = scale(y1, y2)
    ticks.foreach { tick =>
      val py = yscale(tick.v)
      g.drawLine(x1, py, x1 + tickMarkLength, py)

      if (plotDef.showTickLabels) {
        val txt = Text(
          tick.label,
          font = ChartSettings.smallFont,
          alignment = TextAlignment.LEFT,
          style = style
        )
        val txtH = ChartSettings.smallFontDims.height
        val ty = py - txtH / 2
        txt.draw(g, x1 + tickMarkLength + 1, ty, x2, ty + txtH)
      }
    }
  }

  private def drawWithOffset(
    ticks: List[ValueTick],
    g: Graphics2D,
    x1: Int,
    y1: Int,
    x2: Int,
    y2: Int
  ): Unit = {
    val offset = ticks.head.v
    val prefix = tickPrefix(ticks.last.v - offset)
    val offsetStr = prefix.format(offset, tickLabelFmt)
    val offsetTxt =
      Text(
        offsetStr,
        font = ChartSettings.smallFont,
        alignment = TextAlignment.RIGHT,
        style = style
      )
    val offsetH = ChartSettings.smallFontDims.height * 2
    val offsetW = ChartSettings.smallFontDims.width * (offsetStr.length + 3)

    val yscale = scale(y1, y2)
    val oy = yscale(offset)
    val otop = oy - offsetW - tickMarkLength
    val obottom = oy - tickMarkLength
    drawLabel(offsetTxt, g, x1 + tickMarkLength, otop, x1 + offsetH + tickMarkLength, obottom)
    g.drawLine(x1 + offsetH + tickMarkLength, oy, x1, oy)

    ticks.tail.foreach { tick =>
      val py = yscale(tick.v)
      g.drawLine(x1, py, x1 + tickMarkLength, py)

      if (plotDef.showTickLabels) {
        val label = s"+${prefix.format(tick.v - offset, tickLabelFmt)}"
        val txt =
          Text(label, font = ChartSettings.smallFont, alignment = TextAlignment.LEFT, style = style)
        val txtH = ChartSettings.smallFontDims.height
        val ty = py - txtH / 2
        if (ty + txtH < otop)
          txt.draw(g, x1 + tickMarkLength + 1, ty, x2, ty + txtH)
      }
    }
  }
}

object ValueAxis {

  val labelHeight = ChartSettings.normalFontDims.height

  /**
    * Width of value tick labels. The assumption is a monospace font with 7 characters. The 7 is
    * for:
    *
    * - `[sign][3digits][decimal point][1digit][suffix]`: e.g., `-102.3K`
    * - `-1.0e-5`
    */
  val tickLabelWidth = ChartSettings.smallFontDims.width * 7

  val tickMarkLength = 4

  val minTickLabelHeight = ChartSettings.smallFontDims.height * 3
}
