package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette

import java.awt.Color
import java.awt.Graphics2D

trait HeatMap {

  def addLine(line: LineDef): Unit

  def draw(g: Graphics2D): Unit

  def query: String

//  def cmin: Long
//
//  def cmax: Long

  def l: Double

  def u: Double

  def enforceBounds: Unit

  def firstLine: LineDef

  var legendMinMax: Array[(Double, Double, Long)] = null

  def `type`: String

  def yticks: List[ValueTick]

  def counts: Array[Array[Double]]

  def updateLegendMM(count: Double, scaleIndex: Int): Unit = {
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
    updateLegendMM(dp, scaled)
    palette.uniqueColors(scaled)
  }
}

case class CellColor(
  color: Color,
  alpha: Int,
  min: Double,
  max: Double
)
