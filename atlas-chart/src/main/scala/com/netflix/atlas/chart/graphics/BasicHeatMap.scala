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
import com.netflix.atlas.chart.model.HeatmapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale

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
  query: String,
  leftOffset: Int = 0,
  rightOffset: Int = 0
) extends HeatMap {

  val yticks = axis.ticks(y1, chartEnd)
  var lowerCellBound: Double = 0
  var upperCellBound: Double = 0

  private val buckets =
    new Array[Array[Double]](yticks.size + 1) // plus 1 for the data above the final tick
  private val xTicks = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  private val hCells = xTicks.size + 1

  private var minCount = Double.MaxValue
  private var maxCount = Double.MinValue
  private var firstLine: LineDef = null

  def counts: Array[Array[Double]] = buckets

  def enforceBounds: Unit = {
    lowerCellBound = plot.heatmapDef.getOrElse(defaultDef).lower.lower(false, minCount)
    upperCellBound = plot.heatmapDef.getOrElse(defaultDef).upper.upper(false, maxCount)
    if (lowerCellBound > minCount || upperCellBound < maxCount) {
      buckets.foreach { row =>
        for (i <- 0 until row.length) {
          val count = row(i)
          if (count < lowerCellBound || count > upperCellBound) {
            row(i) = 0
          }
        }
      }
    }
  }

  lazy val palette = choosePalette(firstLine)

  lazy val colorScaler = {
    if (upperCellBound < 1) {
      // only linear really makes sense here.
      Scales.linear(lowerCellBound, upperCellBound, 0, palette.uniqueColors.size - 1)
    } else {
      plot.heatmapDef.getOrElse(HeatmapDef()).scale match {
        case Scale.LINEAR =>
          Scales.linear(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.LOGARITHMIC =>
          Scales.logarithmic(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.POWER_2 =>
          Scales.power(2)(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.SQRT =>
          Scales.power(0.5)(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.PERCENTILE =>
          Scales.linear(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
      }
    }
  }

  def addLine(line: LineDef): Unit = {
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

  def draw(g: Graphics2D): Unit = {
    enforceBounds
    System.out.println("***** Draw heatmap")
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

  override def `type`: String = "heatmap"
}
