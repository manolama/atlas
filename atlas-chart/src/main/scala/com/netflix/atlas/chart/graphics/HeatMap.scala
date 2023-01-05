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

import com.netflix.atlas.chart.model.HeatmapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette

import java.awt.Color
import java.awt.Graphics2D

trait HeatMap {

  var legendMinMax: Array[(Double, Double, Long)] = null

  def addLine(line: LineDef): Unit

  def draw(g: Graphics2D): Unit

  def query: String

  def lowerCellBound: Double

  def upperCellBound: Double

  def enforceBounds: Unit

  def `type`: String

  def yticks: List[ValueTick]

  def counts: Array[Array[Double]]

  def updateLegend(count: Double, scaleIndex: Int): Unit = {
    if (legendMinMax == null) {
      legendMinMax = new Array[(Double, Double, Long)](palette.uniqueColors.size)
      for (i <- 0 until legendMinMax.length) legendMinMax(i) = (Long.MaxValue, Long.MinValue, 0)
    }
    val (n, a, c) = legendMinMax(scaleIndex)
    val nn = if (count < n) count else n
    val aa = if (count > a) count else a
    legendMinMax(scaleIndex) = (nn, aa, c + 1)
  }
  def palette: Palette

  def colorScaler: Scales.DoubleScale

  def colorMap: List[CellColor] = {
    palette.uniqueColors.reverse
      .zip(legendMinMax)
      // get rid of colors that weren't used.
      .filterNot(t => t._2._1 == Double.MaxValue && t._2._2 == Double.MinValue)
      // .reverse
      .map { tuple =>
        CellColor(tuple._1, 255, tuple._2._1, tuple._2._2)
      }
  }

  def getColor(dp: Double): Color = {
    val scaled = colorScaler(dp)
    updateLegend(dp, scaled)
    palette.uniqueColors(scaled)
  }
}

case class CellColor(
  color: Color,
  alpha: Int,
  min: Double,
  max: Double
)

object HeatMap {

  // Used to scale a single color on the alpha axis.
  val singleColorAlphas = Array(33, 55, 77, 99, 0xBB, 0xDD, 0xFF)

  val defaultDef = HeatmapDef()

  def choosePalette(line: LineDef): Palette = {
    line.palette.getOrElse {
      val colors = new Array[Color](singleColorAlphas.length)
      for (i <- 0 until colors.length) {
        colors(i) = new Color(
          line.color.getRed,
          line.color.getGreen,
          line.color.getBlue,
          singleColorAlphas(i)
        )
      }
      Palette.fromArray("HeatMap", colors)
    }
  }
}
