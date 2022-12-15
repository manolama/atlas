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

import java.awt.BasicStroke
import java.awt.Color
import java.awt.Graphics2D
import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.graphics.TimeSeriesGraph.bktIdx
import com.netflix.atlas.chart.graphics.TimeSeriesGraph.bktNanos
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.spectator.api.histogram.PercentileBuckets

import scala.collection.mutable

/**
  * Draws a time series graph.
  */
case class TimeSeriesGraph(graphDef: GraphDef) extends Element with FixedHeight with FixedWidth {

  override def height: Int = {
    val max = GraphConstants.MaxHeight
    val h =
      if (graphDef.height > max) max
      else {
        val min = GraphConstants.MinCanvasHeight
        if (graphDef.height < min) min else graphDef.height
      }
    if (graphDef.onlyGraph || graphDef.layout.isFixedHeight) h
    else {
      h + timeAxes.map(_.height).sum
    }
  }

  override def width: Int = {
    val max = GraphConstants.MaxWidth
    val w =
      if (graphDef.width > max) max
      else {
        val min = GraphConstants.MinCanvasWidth
        if (graphDef.width < min) min else graphDef.width
      }
    if (graphDef.onlyGraph || graphDef.layout.isFixedWidth) w
    else {
      val rightPadding = if (yaxes.tail.nonEmpty) 0 else TimeSeriesGraph.minRightSidePadding
      w + yaxes.map(_.width).sum + rightPadding
    }
  }

  val start: Long = graphDef.startTime.toEpochMilli
  val end: Long = graphDef.endTime.toEpochMilli

  val timeAxes: List[TimeAxis] = graphDef.timezones.zipWithIndex.map {
    case (tz, i) =>
      TimeAxis(
        Style(color = graphDef.theme.axis.line.color),
        start,
        end,
        graphDef.step,
        tz,
        if (i == 0) 40 else 0xFF
      )
  }

  val timeAxis: TimeAxis = timeAxes.head

  val yaxes: List[ValueAxis] = graphDef.plots.zipWithIndex.map {
    case (plot, i) =>
      val bounds = plot.bounds(start, end)
//      if (plot.lines.find(_.lineStyle == LineStyle.HEAT).nonEmpty) {
//        // TODO - assuming percentiles here
//        HeatMapTimerValueAxis(
//          plot,
//          graphDef.theme.axis,
//          bktIdx(plot.lines.head),
//          bktIdx(plot.lines.last)
//        )
//      } else
      if (i == 0)
        LeftValueAxis(plot, graphDef.theme.axis, bounds._1, bounds._2)
      else
        RightValueAxis(plot, graphDef.theme.axis, bounds._1, bounds._2)
  }

  private def clip(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    g.setClip(x1, y1, x2 - x1, y2 - y1)
    g.setColor(graphDef.theme.canvas.background.color)
    g.fillRect(x1, y1, x2 - x1, y2 - y1)
  }

  val GENERIC = true

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val leftAxisW = yaxes.head.width
    val rightAxisW = yaxes.tail.foldLeft(0) { (acc, axis) =>
      acc + axis.width
    }
    val rightSideW = if (rightAxisW > 0) rightAxisW else TimeSeriesGraph.minRightSidePadding
    val axisW = leftAxisW + rightSideW
    val width = x2 - x1 - axisW

    val showAxes = !graphDef.onlyGraph && width >= GraphConstants.MinCanvasWidth
    val leftOffset = if (showAxes) leftAxisW else TimeSeriesGraph.minRightSidePadding
    val rightOffset = if (showAxes) rightSideW else TimeSeriesGraph.minRightSidePadding

    val timeAxisH = if (graphDef.onlyGraph) 10 else timeAxis.height
    val timeGrid = TimeGrid(timeAxis, graphDef.theme.majorGrid.line, graphDef.theme.minorGrid.line)

    val chartEnd = y2 - timeAxisH * timeAxes.size

    val prevClip = g.getClip
    clip(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd + 1)
    graphDef.plots.zip(yaxes).foreach {
      case (plot, axis) =>
        val offsets = TimeSeriesStack.Offsets(timeAxis)
        if (plot.lines.find(_.lineStyle == LineStyle.HEATMAP).nonEmpty) {
          // assume all lines in the group will be for a heat map
          if (GENERIC) {
            val yticks = axis.ticks(y1, chartEnd)
            val buckets =
              new Array[Array[Long]](yticks.size + 1) // plus 1 for the data above the final tick
            val hCells = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset).size + 1
            val yHeight = (chartEnd - y1) / yticks.size
            val bucketScaler = axis.scale(0, buckets.length)
            val xScaler = timeAxis.scale(x1 + leftOffset, x2 - rightOffset)
            var cmin = Long.MaxValue
            var cmax = Long.MinValue
            System.out.println(s"# Ticks ${yticks.size} vs # Buckets ${buckets.length}")
            // TODO - not every line will be for a heatmap for a plot! Plots are grouped on
            // the yaxis.
            plot.lines.foreach { line =>
              var t = graphDef.startTime.toEpochMilli
              val ti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset).iterator
              var lastTick = ti.next()
              var bi = 0
              while (t < graphDef.endTime.toEpochMilli) {
                val v = line.data.data(t)
                // val y = buckets.size - bucketScaler(v)
                var y = 0
                val yi = yticks.iterator
                var found = false
                while (yi.hasNext && !found) {
                  val ti = yi.next()
                  if (v < ti.v) {
                    found = true
                  } else {
                    y += 1
                  }
                }
                if (!found) {
                  y = buckets.length - 1
                }
                val x = if (t < lastTick.timestamp) {
                  bi
                } else {
                  bi += 1
                  if (ti.hasNext) lastTick = ti.next()
                  bi
                }
                // System.out.println(s" Y: ${y}   X: ${x}")
                if (x < hCells && y < buckets.length) {
                  var b = buckets(y)
                  if (b == null) {
                    b = new Array[Long](hCells)
                    buckets(y) = b
                  }
                  b(x) += 1
                  if (b(x) > 0 && b(x) > cmax) {
                    cmax = b(x)
                  }
                  if (b(x) > 0 && b(x) < cmin) {
                    cmin = b(x)
                  }
                }
                t += graphDef.step
              }
            }

            // TODO - move, for now, try it
            val palette = plot.lines.head.palette.getOrElse(
              Palette.singleColor(plot.lines.head.color)
            )
            val colorScaler =
              Scales.factory(Scale.LOGARITHMIC)(cmin, cmax, 0, palette.uniqueColors.size - 1)
            var last = 0
            val yScaler = axis.scale(y1, chartEnd)
            val yi = yticks.iterator
            var lastY = chartEnd
            buckets.foreach { bucket =>
              val ytick = if (yi.hasNext) yi.next() else null
              val nextY = if (ytick != null) yScaler(ytick.v) else y1
              if (bucket != null) {
                val lineElement = HeatmapLine(bucket, timeAxis, colorScaler, palette)
                lineElement.draw(g, x1 + leftOffset, nextY, x2 - rightOffset, lastY - nextY)
              }
              lastY = nextY
            }
//            buckets.zip(yticks).foreach { tuple =>
//              val (bucket, ytick) = tuple
//              val y = yScaler(ytick.v)
//              // System.out.println(s"Last ${last} new Y ${y}")
//              if (bucket != null) {
//                val lineElement = HeatmapLine(bucket, timeAxis, colorScaler, palette)
//                lineElement.draw(g, x1 + leftOffset, y, x2 - rightOffset, yHeight)
//              }
//              last = y
//            }
//            // final bucket
//            val bucket = buckets.last
//            if (bucket != null) {
//              val lineElement = HeatmapLine(bucket, timeAxis, colorScaler, palette)
//              lineElement.draw(g, x1 + leftOffset, last - yHeight, x2 - rightOffset, yHeight)
//            }

//            Style(Color.RED).configure(g)
//            g.drawLine(x1, 305, x2, 305)

          } else {
            val minBktIdx = bktIdx(plot.lines.head)
            val maxBktIdx = bktIdx(plot.lines.last)
            val ypixels = chartEnd - y1
            val bktRange =
              bktIdx(plot.lines.last) - bktIdx(plot.lines.head)
            val dpHeight = ypixels.toDouble / bktRange
            // val yFudge = Math.round(bktRange.toDouble / (ypixels - (bktRange * dpHeight)))
            // System.out.println(s"dpHeight ${dpHeight}  yFudge: ${yFudge}")
            System.out.println(s"YPixels: ${ypixels}  Bkt Range ${bktRange}")
            var cmin = Long.MaxValue
            var cmax = Long.MinValue
            val numDps =
              (graphDef.endTime.toEpochMilli - graphDef.startTime.toEpochMilli) / graphDef.step
            val combinedSeries = new mutable.TreeMap[Int, (Long, Array[Double])]
            plot.lines.foreach { line =>
              val idx = bktIdx(line)
              val (_, arr) =
                combinedSeries
                  .getOrElseUpdate(idx, bktNanos(line) -> new Array[Double](numDps.toInt))
              var t = graphDef.startTime.toEpochMilli
              var di = 0
              while (t < graphDef.endTime.toEpochMilli) {
                val v = line.data.data(t).toLong
                arr(di) += v
                if (arr(di) > cmax) cmax = arr(di).toLong
                if (arr(di) < cmin) cmin = arr(di).toLong
                di += 1
                t += line.data.data.step
              }
            }

            val reds = Palette.fromResource("bluegreen")
            var redList = List.empty[Color]
            for (i <- 0 until 7) {
              redList ::= reds.colors(i)
            }
            val colorScaler = Scales.factory(Scale.LOGARITHMIC)(cmin, cmax, 0, redList.size - 1)

            var previousOffset = chartEnd.toDouble + 1

            for (i <- minBktIdx until maxBktIdx) {
              val nextOffset = previousOffset - dpHeight
              val h = (Math.round(previousOffset) - Math.round(nextOffset)).toInt
              val offset = Math.round(previousOffset).toInt - h
              previousOffset = nextOffset
              val sec = PercentileBuckets.get(i).toDouble / 1000 / 1000 / 1000
              val prefix = Ticks.getDurationPrefix(sec, sec)
              val fmt = prefix.format(sec, "%.1f%s")
              val label = prefix.format(sec, fmt)
              //            if (yH - h < 0) {
              //              throw new IllegalStateException(s"Negative Y!!! ${yH - h}")
              //            }
              //            System.out.println(s"  Cell height: ${h} @ ${i}  Offset ${offset} -> ${label}")
              combinedSeries.get(i) match {
                case Some((nanos, line)) =>
                  // System.out.println(s"YAY  @idx ${i} @offset ${yH - h} H: ${h}")
                  val ts =
                    new ArrayTimeSeq(DsType.Gauge, graphDef.startTime.toEpochMilli, 60_000, line)
                  val style = Style(color = Color.RED, stroke = new BasicStroke(1))
                  val lineElement =
                    PTileHistoLine(
                      style,
                      ts,
                      timeAxis,
                      axis,
                      cmax - cmin,
                      offset,
                      h,
                      colorScaler,
                      redList
                    )
                  lineElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
                case None => // no-op
                // System.out.println(s"nope @idx ${i} @offset ${yH - h} H: ${h}")
              }
              // ctr += 1
              // yH -= h
            }
            if (previousOffset > 2) {
              // throw new IllegalStateException(s"Too many pixels un-used!!!! ${yH}")
            }
            System.out.println(s" ####### Final yh: ${previousOffset - y1}")
          }
        } else {
          plot.lines.foreach { line =>
            val style = Style(color = line.color, stroke = new BasicStroke(line.lineWidth))
            val lineElement = line.lineStyle match {
              case LineStyle.LINE  => TimeSeriesLine(style, line.data.data, timeAxis, axis)
              case LineStyle.AREA  => TimeSeriesArea(style, line.data.data, timeAxis, axis)
              case LineStyle.VSPAN => TimeSeriesSpan(style, line.data.data, timeAxis)
              case LineStyle.STACK =>
                TimeSeriesStack(style, line.data.data, timeAxis, axis, offsets)
            }

            lineElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
          }
        }

        plot.horizontalSpans.foreach { hspan =>
          val style = Style(color = hspan.color)
          val spanElement = ValueSpan(style, hspan.v1, hspan.v2, axis)
          spanElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
        }

        plot.verticalSpans.foreach { vspan =>
          val style = Style(color = vspan.color)
          val spanElement = TimeSpan(style, vspan.t1.toEpochMilli, vspan.t2.toEpochMilli, timeAxis)
          spanElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
        }
    }
    g.setClip(prevClip)

    timeGrid.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)

    if (!graphDef.onlyGraph) {
      timeAxes.zipWithIndex.foreach {
        case (axis, i) =>
          val offset = chartEnd + 1 + timeAxisH * i
          axis.draw(g, x1 + leftOffset, offset, x2 - rightOffset, y2)
      }
    }

    val valueGrid =
      ValueGrid(yaxes.head, graphDef.theme.majorGrid.line, graphDef.theme.minorGrid.line)
    valueGrid.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
    if (showAxes) {
      yaxes.head.draw(g, x1, y1, x1 + leftAxisW - 1, chartEnd)
      yaxes.tail.zipWithIndex.foreach {
        case (axis, i) =>
          val offset = leftAxisW + width + leftAxisW * i
          axis.draw(g, x1 + offset, y1, x1 + offset + leftAxisW, chartEnd)
      }
    }
  }
}

object TimeSeriesGraph {

  /**
    * Allow at least 4 small characters on the right side to prevent the final tick mark label
    * from getting truncated.
    */
  private[graphics] val minRightSidePadding = ChartSettings.smallFontDims.width * 4

  private[graphics] def bktNanos(line: LineDef): Long = {
    PercentileBuckets.get(bktIdx(line))
  }

  private[graphics] def bktIdx(line: LineDef): Int = {
    Integer.parseInt(line.data.tags("percentile").substring(1), 16)
  }
}
