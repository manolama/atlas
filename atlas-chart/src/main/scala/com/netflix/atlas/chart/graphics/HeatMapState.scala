package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette

import java.awt.Color
import java.awt.Graphics2D

trait HeatMapState {

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

  def legendMinMax: Array[(Long, Long, Long)]

  def `type`: String

  def yticks: List[ValueTick]

  def counts: Array[Array[Long]]

  def updateLegendMM(count: Long, scaleIndex: Int): Unit

  def palette: Palette

  def colorScaler: Scales.DoubleScale

  def colorMap: List[CellColor]

  def getColor(dp: Long): Color
}

case class CellColor(
  color: Color,
  alpha: Int,
  min: Long,
  max: Long
)
