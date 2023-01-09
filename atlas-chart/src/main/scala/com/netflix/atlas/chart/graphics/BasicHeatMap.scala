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

  private[graphics] val buckets = new Array[Array[Double]](yticks.size)
  private[graphics] val xTicks = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  private[graphics] val hCells = xTicks.size

  private[graphics] var minCount = Double.MaxValue
  private[graphics] var maxCount = Double.MinValue
  private[graphics] var lowerCellBound: Double = 0
  private[graphics] var upperCellBound: Double = 0
  private[graphics] var firstLine: LineDef = null
  private[graphics] var label: String = null
  private[graphics] var bucketScaler = axis.scale(0, buckets.length - 1)

  // ctor
  {
    for (i <- 0 until buckets.length) {
      buckets(i) = new Array[Double](hCells)
    }
    plot.lines.zip(plot.data).filter(_._1.lineStyle == LineStyle.HEATMAP).foreach { tuple =>
      val (line, data) = tuple
      if (firstLine == null) {
        firstLine = line
        // TODO - if the user explicitly provides a label via :legend, naturally
        // we'll use that. But it will only grab the FIRST of the expressions in the
        // heatmap. If there is no label, we can try falling back to the first query.
        // Failing that, the ylabel then just the string "Heatmap".
        if (data.label != null && data.label.nonEmpty) {
          label = data.label
        } else {
          label = line.query.getOrElse(plot.ylabel.getOrElse("Heatmap"))
        }
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
    var t = graphDef.startTime.toEpochMilli
    val ti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset).iterator
    var lastTick = ti.next()
    var bi = 0
    while (t < graphDef.endTime.toEpochMilli) {
      val v = line.data.data(t)
      if (v.isFinite) {
        val y = (buckets.length - 1) - bucketScaler(v)
        val x = if (t <= lastTick.timestamp) {
          bi
        } else {
          if (ti.hasNext) lastTick = ti.next()
          bi += 1
          bi
        }

        // safety check
        if (x < hCells && y < buckets.length) {
          val b = buckets(y)
          b(x) += 1
          if (b(x) > maxCount) maxCount = b(x)
          if (b(x) < minCount) minCount = b(x)
        }
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
