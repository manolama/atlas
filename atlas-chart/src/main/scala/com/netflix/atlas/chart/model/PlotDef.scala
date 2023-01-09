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

import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktNanos
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.isSpectatorPercentile

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

  def bounds(start: Long, end: Long): (Double, Double) = {

    val dataLines = lines
    if (dataLines.isEmpty) 0.0 -> 1.0
    else {
      val step = dataLines.head.data.data.step
      // TODO - oohhhhewwwwwwwweeewewewewe how would STACK lines interact with
      // Percentile graphs...? Blech!!!!!
      val (regular, stacked) = dataLines
        .filter(_.lineStyle != LineStyle.VSPAN)
        .partition(_.lineStyle != LineStyle.STACK)

      var max = -JDouble.MAX_VALUE
      var min = JDouble.MAX_VALUE
      var posSum = 0.0
      var negSum = 0.0

      var t = start
      while (t < end) {
        regular.foreach { line =>
          val v = line.lineStyle match {
            case LineStyle.HEATMAP =>
              // TODO - super duper inefficient since we only need to get the bucket seconds ONCE per
              // line. meh.
              if (isSpectatorPercentile(line)) {
                val bs = bktSeconds(line)
                // System.out.println(s"***** BS: ${bs}  Idx: ${bktIdx(line)}")
                bs
              } else {
                line.data.data(t)
              }
            case _ => line.data.data(t)
          }
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

      // TODO fun edge case for ptile buckets. IF we have one bucket, the max and min are the
      // same. This will cause a 1 to be added and throw off bucketing. For now, if all of
      // the lines are heatmaps and percentiles, do some fudging.
      val ptileOverride =
        if (
          max == min &&
          lines
            .find(l =>
              l.lineStyle != LineStyle.HEATMAP || (l.lineStyle == LineStyle.HEATMAP && !isSpectatorPercentile(
                l
              ))
            )
            .isEmpty
        ) {
          true
        } else false

      min = if (min == JDouble.MAX_VALUE) 0.0 else min
      max = if (max == -JDouble.MAX_VALUE) 1.0 else max
      finalBounds(hasArea, min, max, ptileOverride)
    }
  }

  private[model] def finalBounds(
    hasArea: Boolean,
    min: Double,
    max: Double,
    ptileOverride: Boolean = false
  ): (Double, Double) = {

    // Try to figure out bounds following the guidelines:
    // * An explicit bound should always get used.
    // * If an area is present, then automatic bounds should go to the 0 line.
    // * If an automatic bound equals or is on the wrong side of an explicit bound, then pad by 1.
    val l = lower.lower(hasArea, min)
    val u = upper.upper(hasArea, max)

    // If upper and lower bounds are equal or automatic/explicit combination causes lower to be
    // greater than the upper, then pad automatic bounds by 1. Explicit bounds should
    // be honored.
    if (l < u) l -> u
    else {
      (lower, upper) match {

        case (Explicit(_), Explicit(_)) => l       -> u
        case (_, Explicit(_))           => (u - 1) -> u
        case (Explicit(_), _) =>
          if (ptileOverride)
            l -> l
          else
            l -> (l + 1)
        case (_, _) =>
          if (ptileOverride)
            l -> u
          else
            l -> (u + 1)
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
