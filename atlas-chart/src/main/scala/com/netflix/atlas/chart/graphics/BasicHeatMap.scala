/*
 * Copyright 2022-2023 Netflix, Inc.
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

import com.netflix.atlas.chart.graphics.HeatMap.choosePalette
import com.netflix.atlas.chart.graphics.HeatMap.defaultDef
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.isSpectatorPercentile
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.PlotDef

import java.awt.Graphics2D

case class BasicHeatMap(
  graphDef: GraphDef,
  plot: PlotDef,
  axis: ValueAxis,
  timeAxis: TimeAxis,
  x1: Int,
  y1: Int,
  x2: Int,
  chartEnd: Int,
  leftOffset: Int = 0,
  rightOffset: Int = 0
) extends HeatMap {

  val yticks = axis.ticks(y1, chartEnd)

  private[graphics] val buckets =
    new Array[Array[Double]](yticks.size + 1) // plus 1 for the data above the final tick
  private[graphics] val xTicks = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  private[graphics] val hCells = xTicks.size + 1

  private[graphics] var minCount = Double.MaxValue
  private[graphics] var maxCount = Double.MinValue
  private[graphics] var lowerCellBound: Double = 0
  private[graphics] var upperCellBound: Double = 0
  private[graphics] var firstLine: LineDef = null
  private[graphics] var label: String = null

  {
    plot.lines.filter(_.lineStyle == LineStyle.HEATMAP).foreach { line =>
      if (firstLine == null) {
        firstLine = line
        label = line.query.getOrElse("")
      }
      addLine(line)
    }
    enforceCellBounds
  }

  override def draw(g: Graphics2D): Unit = {
    val yScaler = axis.scale(y1, chartEnd)
    val yi = yticks.iterator
    var lastY = chartEnd + 1
    buckets.foreach { bucket =>
      val ytick = if (yi.hasNext) yi.next() else null
      val nextY = if (ytick != null) yScaler(ytick.v) else y1
      if (bucket != null) {
        val lineElement = HeatmapLine(bucket, timeAxis, this)
        lineElement.draw(g, x1 + leftOffset, nextY, x2 - rightOffset, lastY - nextY)
      }
      lastY = nextY
    }
  }

  override def legendLabel: String = label

  override def rows: Array[Array[Double]] = buckets

  override def `type`: String = "heatmap"

  protected[graphics] lazy val palette = choosePalette(firstLine)

  protected[graphics] lazy val colorScaler =
    HeatMap.colorScaler(plot, palette, lowerCellBound, upperCellBound)

  private def addLine(line: LineDef): Unit = {
    val seconds = if (isSpectatorPercentile(line)) bktSeconds(line) else -1
    if (firstLine == null) {
      firstLine = line
    }
    var t = graphDef.startTime.toEpochMilli
    val ti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset).iterator
    var lastTick = ti.next()
    var bi = 0
    while (t < graphDef.endTime.toEpochMilli) {
      val v = line.data.data(t)
      val cmp = if (seconds >= 0) seconds else v
      var y = 0
      val yi = yticks.iterator
      var found = false
      while (yi.hasNext && !found) {
        val ti = yi.next()
        if (cmp <= ti.v) {
          found = true
        } else {
          y += 1
        }
      }
      if (!found) {
        y = buckets.length - 1
      }

      val x = if (t <= lastTick.timestamp) {
        bi
      } else {
        if (ti.hasNext) lastTick = ti.next()
        bi += 1
        bi
      }

      if (x < hCells && y < buckets.length) {
        var b = buckets(y)
        if (b == null) {
          b = new Array[Double](hCells)
          buckets(y) = b
        }
        if (seconds >= 0) b(x) += v.toLong else b(x) += 1
        if (b(x) > 0 && b(x) > maxCount) {
          maxCount = b(x)
        }
        if (b(x) > 0 && b(x) < minCount) {
          minCount = b(x)
        }
      } else {
        System.out.println(
          s"************** WTF? X ${x} vs ${hCells}, Y ${y} vs ${buckets.length} @ ${t}"
        )
      }
      t += graphDef.step
    }
  }

  private def enforceCellBounds: Unit = {
    lowerCellBound = plot.heatmapDef.getOrElse(defaultDef).lower.lower(false, minCount)
    upperCellBound = plot.heatmapDef.getOrElse(defaultDef).upper.upper(false, maxCount)
    buckets.foreach { row =>
      for (i <- 0 until row.length) {
        val count = row(i)
        if (count < lowerCellBound || count > upperCellBound) {
          row(i) = 0
        } else {
          updateLegend(count, colorScaler(count))
        }
      }
    }
  }
}
