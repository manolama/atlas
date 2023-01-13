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
package com.netflix.atlas.chart.model

import com.netflix.atlas.chart.graphics.PercentileHeatmap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatmap.bktNanos
import com.netflix.atlas.chart.graphics.PercentileHeatmap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatmap.isSpectatorPercentile

import java.awt.Color
import com.netflix.atlas.chart.graphics.Theme
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model.PlotBound.Explicit

/**
  * Definition for a plot, i.e., a y-axis and associated data elements.
  *
  * @param data
  *     List of data items to include in the plot.
  * @param ylabel
  *     Label to show for the axis.
  * @param axisColor
  *     Color to use when rendering the axis.
  * @param scale
  *     Type of scale to use on the axis, linear or logarithmic.
  * @param upper
  *     Upper limit for the axis.
  * @param lower
  *     Lower limit for the axis.
  * @param tickLabelMode
  *     Mode to use for displaying tick labels.
  * @param palette
  *     Optional palette for the plot.
  * @param heatmapDef
  *     Optional heatmap settings for the plot.
  */
case class PlotDef(
  data: List[DataDef],
  ylabel: Option[String] = None,
  axisColor: Option[Color] = None,
  scale: Scale = Scale.LINEAR,
  upper: PlotBound = AutoStyle,
  lower: PlotBound = AutoStyle,
  tickLabelMode: TickLabelMode = TickLabelMode.DECIMAL,
  palette: Option[Palette] = None,
  heatmapDef: Option[HeatMapDef] = None
) {

  import java.lang.{Double => JDouble}

  (lower, upper) match {
    case (Explicit(l), Explicit(u)) =>
      require(l < u, s"lower bound must be less than upper bound ($l >= $u)")
    case (_, _) =>
  }

  def bounds(start: Long, end: Long): (Double, Double, Int, Int) = {

    val dataLines = lines
    if (dataLines.isEmpty) (0.0, 1.0, -1, -1)
    else {
      val step = dataLines.head.data.data.step
      val (regular, stacked) = dataLines
        .filter(ls =>
          ls.lineStyle != LineStyle.VSPAN &&
          !(ls.lineStyle == LineStyle.HEATMAP && isSpectatorPercentile(ls))
        )
        .partition(_.lineStyle != LineStyle.STACK)

      var max = -JDouble.MAX_VALUE
      var min = JDouble.MAX_VALUE
      var posSum = 0.0
      var negSum = 0.0

      var t = start
      while (t < end) {
        regular.foreach { line =>
          val v = line.data.data(t)
          if (JDouble.isFinite(v)) {
            max = if (v > max) v else max
            min = if (v < min) v else min
          }
        }

        stacked.foreach { line =>
          val v = line.data.data(t)
          if (JDouble.isFinite(v)) {
            if (v >= 0.0) posSum += v else negSum += v
          }
        }

        if (stacked.nonEmpty) {
          val v = stacked.head.data.data(t)
          if (JDouble.isFinite(v)) {
            max = if (v > max) v else max
            min = if (v < min) v else min
          }

          max = if (posSum > 0.0 && posSum > max) posSum else max
          min = if (negSum < 0.0 && negSum < min) negSum else min
        }

        posSum = 0.0
        negSum = 0.0
        t += step
      }

      // If an area or stack is shown it will fill to zero and the filled area should be shown
      val hasArea = dataLines.exists { line =>
        line.lineStyle == LineStyle.AREA || line.lineStyle == LineStyle.STACK
      }

      val percentileBuckets =
        dataLines.filter(ls => ls.lineStyle == LineStyle.HEATMAP && isSpectatorPercentile(ls))
      val minP = percentileBuckets.headOption.map(bktIdx(_)).getOrElse(-1)
      if (minP > -1) {
        val s = bktSeconds(minP)
        if (s > max) max = s
        if (s < min) min = s
      }
      val maxP = percentileBuckets.lastOption.map(bktIdx(_)).getOrElse(-1)
      if (maxP > -1) {
        val s = bktSeconds(maxP)
        if (s > max) max = s
        if (s < min) min = s
      }

      min = if (min == JDouble.MAX_VALUE) 0.0 else min
      max = if (max == -JDouble.MAX_VALUE) 1.0 else max
      finalBounds(hasArea, min, max, minP, maxP)
    }
  }

  private[model] def finalBounds(
    hasArea: Boolean,
    min: Double,
    max: Double,
    minPercentileBkt: Int = -1,
    maxPercentileBkt: Int = -1
  ): (Double, Double, Int, Int) = {

    // Try to figure out bounds following the guidelines:
    // * An explicit bound should always get used.
    // * If an area is present, then automatic bounds should go to the 0 line.
    // * If an automatic bound equals or is on the wrong side of an explicit bound, then pad by 1.
    val l = lower.lower(hasArea, min)
    val u = upper.upper(hasArea, max)

    // If upper and lower bounds are equal or automatic/explicit combination causes lower to be
    // greater than the upper, then pad automatic bounds by 1. Explicit bounds should
    // be honored.
    if (l < u) (l, u, minPercentileBkt, maxPercentileBkt)
    else {
      (lower, upper) match {

        case (Explicit(_), Explicit(_)) => (l, u, minPercentileBkt, maxPercentileBkt)
        case (_, Explicit(_))           => (u - 1, u, minPercentileBkt, maxPercentileBkt)
        case (Explicit(_), _) =>
          if (minPercentileBkt >= 0) (l, l, minPercentileBkt, maxPercentileBkt)
          else (l, l + 1, minPercentileBkt, maxPercentileBkt)
        case (_, _) =>
          if (minPercentileBkt >= 0) (l, u, minPercentileBkt, maxPercentileBkt)
          else (l, u + 1, minPercentileBkt, maxPercentileBkt)
      }
    }
  }

  def getAxisColor(dflt: Color): Color = {
    axisColor.getOrElse(dflt)
  }

  def showTickLabels: Boolean = tickLabelMode != TickLabelMode.OFF

  def horizontalSpans: List[HSpanDef] = data.collect { case v: HSpanDef => v }

  def verticalSpans: List[VSpanDef] = data.collect { case v: VSpanDef => v }

  def lines: List[LineDef] = data.collect { case v: LineDef => v }

  def normalize(theme: Theme): PlotDef = {
    copy(axisColor = Some(getAxisColor(theme.legend.text.color)))
  }
}
