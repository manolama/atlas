package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.LineDef

import java.awt.Graphics2D

trait HeatMapState {

  def addLine(line: LineDef): Unit

  def draw(g: Graphics2D): Unit

  def query: String

  def cmin: Long

  def cmax: Long

  def firstLine: LineDef

  def legendMinMax: Array[(Long, Long)]
}
