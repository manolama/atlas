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

import java.awt.Font
import java.awt.Graphics2D
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.PlotDef

/**
  * Draws a legend for a given plot.
  *
  * @param plot
  *     Plot definition corresponding to the legend.
  * @param label
  *     Overall label to show for this legend.
  * @param showStats
  *     Whether or not to show basic line statistics for the legend entries.
  * @param maxEntries
  *     Maximum number of entries to show in the legend.
  */
case class Legend(
  styles: Styles,
  plot: PlotDef,
  label: Option[String],
  showStats: Boolean,
  maxEntries: Int,
  plotId: Int,
  graph: TimeSeriesGraph
) extends Element
    with VariableHeight {

  private val (numEntries, numHeatMaps) = {
    var lastHeatmapQuery: String = null
    var count = 0
    var heatmaps = 0
    plot.lines.foreach { line =>
      line.lineStyle match {
        case LineStyle.HEATMAP =>
          if (lastHeatmapQuery == null || !lastHeatmapQuery.equals(line.query.getOrElse(""))) {
            lastHeatmapQuery = line.query.getOrElse("")
            count += 1
            heatmaps += 1
          }
        case _ => count += 1
      }
    }
    (count, heatmaps)
  }

  private val header = HorizontalPadding(5) :: label.toList.map { str =>
    val bold = ChartSettings.normalFont.deriveFont(Font.BOLD)
    val headerColor = plot.getAxisColor(styles.text.color)
    Text(str, font = bold, alignment = TextAlignment.LEFT, style = Style(headerColor))
  }

  private val entries = {
    val results = List.newBuilder[Element with Product]
    if (plot.lines.find(_.lineStyle == LineStyle.HEATMAP).nonEmpty) {
      results += HeatMapLegend(styles, plot, plotId, showStats, graph)
    }
    plot.lines.zip(plot.data).filter(_._1.lineStyle != LineStyle.HEATMAP).foreach { tuple =>
      val (_, data) = tuple
      results += HorizontalPadding(2)
      results += LegendEntry(styles, plot, data, showStats)
    }
    results.result()
  }

  private val footer =
    if (numEntries <= maxEntries) Nil
    else {
      val remaining = numEntries - maxEntries
      val txt =
        Text(s"... $remaining suppressed ...", alignment = TextAlignment.LEFT, style = styles.text)
      List(HorizontalPadding(2), txt)
    }

  private val block = Block(header ::: entries ::: footer)

  override def minHeight: Int = block.minHeight

  override def computeHeight(g: Graphics2D, width: Int): Int = block.computeHeight(g, width)

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    block.draw(g, x1, y1, x2, y2)
  }
}
