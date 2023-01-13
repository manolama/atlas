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

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.HeatmapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale

import java.awt.Color
import java.awt.Graphics2D

/**
  * Represents a heatmap for graphing or serializing.
  */
trait Heatmap {

  /** Used to track legend min/max values and hits. (min, max, hits) */
  private var legendMinMax: Array[(Double, Double, Long)] = null

  /**
    * Plots the heatmap after all of the lines have been added.
    *
    * @param g
    *   The non-null graphics context to update.
    */
  def draw(g: Graphics2D): Unit

  /**
    * @return
    *   A label to use for the legend. May be user provided or fall back to a
    *   query.
    */
  def legendLabel: String

  /**
    * @return
    *   The type of heatmap implemented. Use for serialization.
    */
  def `type`: String

  /**
    * @return
    *   The heatmap counts that would be plotted in a graph. Organized by row
    *   (bottom most row first, correlated with the lowest tick value) and time.
    */
  def rows: Array[Array[Double]]

  /**
    * Compiles a map of colors with min, max and hits for use in plotting or serializing
    * a color legend.
    *
    * @return
    * A non-null list of heat map color entries. The list may be empty if no values
    * were recorded (data was all NaN or 0).
    */
  def colorMap: List[HeatMapLegendColor] = {
    if (legendMinMax == null) {
      return List.empty
    }

    palette.uniqueColors.reverse
      .zip(legendMinMax)
      // get rid of colors that weren't used.
      .filter(t => t._2._3 != 0)
      .map { tuple =>
        HeatMapLegendColor(tuple._1, tuple._2._1, tuple._2._2, tuple._2._3)
      }
  }

  /**
    * @return
    *   An instance of Yaxis ticks for this heatmap.
    */
  def yticks: List[ValueTick]

  /**
    * @return
    *   The palette used by this heatmap.
    */
  protected[graphics] def palette: Palette

  /**
    * @return
    *   The color scaler used by this heatmap.
    */
  protected[graphics] def colorScaler: Scales.DoubleScale

  /**
    * Accepts a count for a bucket and returns the color the count maps to. Also
    * updates the legend min/max array.
    *
    * @param count
    *   The datapoint to fetch a color for. Must be finite and greater than 0.
    * @return
    *   The color to plot for the count.
    */
  protected[graphics] def getColor(count: Double): Color = {
    val scaled = colorScaler(count)
    palette.uniqueColors.reverse(scaled)
  }

  private[graphics] def updateLegend(count: Double, scaleIndex: Int): Unit = {
    if (legendMinMax == null) {
      legendMinMax = new Array[(Double, Double, Long)](palette.uniqueColors.size)
      for (i <- 0 until legendMinMax.length) legendMinMax(i) = (Long.MaxValue, Long.MinValue, 0)
    }
    val (n, a, c) = legendMinMax(scaleIndex)
    val nn = if (count < n) count else n
    val aa = if (count > a) count else a
    legendMinMax(scaleIndex) = (nn, aa, c + 1)
  }
}

/**
  * A legend color for serialization.
  *
  * @param color
  *   The color used in the heatmap.
  * @param min
  *   The minimum count actually used for this color.
  * @param max
  *   The maximum count actually used for this color.
  * @param hits
  *   The number of data points that mapped to this color.
  */
case class HeatMapLegendColor(
  color: Color,
  min: Double,
  max: Double,
  hits: Long
)

object Heatmap {

  // Used to scale a single color on the alpha axis.
  val singleColorAlphas = Array(33, 55, 77, 99, 0xBB, 0xDD, 0xFF).reverse

  val defaultDef = HeatmapDef()

  /**
    * Computes the starting position of the graph given optional titles or padding.
    * Mirror of the DefaultGraphEngine code, copied out here for JsonCodec serialization
    * purposes.
    *
    * @param config
    *   The non-null graph config.
    * @return
    *   A positive offset incorporating optional titles.
    */
  def computeGraphY(config: GraphDef): Int = {
    val aboveCanvas = List.newBuilder[Element]

    config.title.foreach { str =>
      if (config.showText)
        aboveCanvas += Text(str, font = ChartSettings.largeFont, style = config.theme.image.text)
          .truncate(config.width)
    }
    aboveCanvas += HorizontalPadding(5)

    val h = aboveCanvas.result().foldLeft(0) { (acc, e) =>
      acc + e.getHeight(ChartSettings.refGraphics, config.width)
    }
    if (config.layout.isFixedHeight) h else 0
  }

  /**
    * Determines the palette to use from a line definition. If the line includes
    * a palette, that palette takes precedence. If that palette only has one color,
    * we apply the alpha list to the color.
    * If no palette is present, we use the line def color and apply the aplha list.
    *
    * @param line
    *   A non-null line with an optional palette applied. The color is required and
    *   set by default.
    * @return
    *   A palette to use for the color scale.
    */
  def choosePalette(line: LineDef): Palette = {
    line.palette match {
      case Some(p) =>
        if (p.uniqueColors.length > 1) p
        else fromSingleColor(p.uniqueColors.head)
      case None => fromSingleColor(line.color)
    }
  }

  /**
    * Returns a scale for the heatmap color range scaled to the cell count against
    * the number of unique colors in the palette. The requested color scaling is used
    * for upper cell counts of 1 or more. If the upper cell count is less than 1,
    * the scale switches to linear to avoid using a single color.
    *
    * **NOTE** if the scale is set to PERCENTILE for some odd reason, (shouldn't
    * be used on the color scale) we switch to log.
    * 
    * @param plot
    *   The non-null plot used to pull the heat map color scaling type.
    * @param palette
    *   The non-null palette with at least 2 colors.
    * @param lowerCellBound
    *   The lower cell boundary. Should be greater than zero.
    * @param upperCellBound
    *   The upper cell boundary. Must be greater than or equal to the lower cell bound.
    * @return
    *   A scaler that returns the index of the color to use in the palette's unique
    *   color list.
    */
  def colorScaler(
    plot: PlotDef,
    palette: Palette,
    lowerCellBound: Double,
    upperCellBound: Double
  ): Scales.DoubleScale = {
    if (upperCellBound < 1) {
      // only linear really makes sense here.
      Scales.linear(lowerCellBound, upperCellBound, 0, palette.uniqueColors.size - 1)
    } else {
      plot.heatmapDef.getOrElse(defaultDef).colorScale match {
        case Scale.LINEAR =>
          Scales.linear(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.LOGARITHMIC =>
          Scales.logarithmic(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.POWER_2 =>
          Scales.power(2)(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.SQRT =>
          Scales.power(0.5)(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
        case Scale.PERCENTILE =>
          // Not particularly useful so we'll switch to log.
          Scales.logarithmic(lowerCellBound, upperCellBound + 1, 0, palette.uniqueColors.size)
      }
    }
  }

  private def fromSingleColor(color: Color): Palette = {
    val colors = new Array[Color](singleColorAlphas.length)
    for (i <- 0 until colors.length) {
      colors(i) = new Color(
        color.getRed,
        color.getGreen,
        color.getBlue,
        singleColorAlphas(i)
      )
    }
    Palette.fromArray("HeatMap", colors)
  }

}
