package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.util.UnitPrefix

import java.awt.Color
import java.awt.Graphics2D

case class HeatMapLegend(
  styles: Styles,
  plot: PlotDef,
  showStats: Boolean,
  query: String
) extends Element
    with VariableHeight {

  def addLine(line: LineDef): Unit = {}

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
//    val width = x2 - x1
//
//    val reds = Palette.fromResource("bluegreen")
//    var redList = List.empty[Color]
//    for (i <- 0 until 7) {
//      redList ::= reds.colors(i)
//    }
//
//    val colorWidth = 15
//    val blockWidth = redList.size * colorWidth
//    val center = width / 2
//    var blockX = center - (blockWidth / 2)
//    val blockY = y1
//    redList.foreach { c =>
//      Style(c).configure(g)
//      g.fillRect(blockX, blockY, colorWidth, colorWidth)
//      blockX += colorWidth
//    }
//
//    blockX = center - (blockWidth / 2)
//
//    val txtY = y1 + colorWidth + 5
//    val txtH = ChartSettings.smallFontDims.height
//    System.out.println(s"TEXT FOR MIN: [${UnitPrefix.format(cmin)}]")
//    var txt = Text(
//      UnitPrefix.format(cmin),
//      font = ChartSettings.smallFont,
//      alignment = TextAlignment.CENTER
//    )
//
//    var txtX = center - (blockWidth / 2)
//    val w = center - txtX
//
//    txt.draw(g, txtX - w, txtY, txtX + w, txtY + txtH)
//
//    txt = Text(
//      UnitPrefix.format(cmax),
//      font = ChartSettings.smallFont,
//      alignment = TextAlignment.CENTER
//    )
//    txtX = center + w
//    txt.draw(g, txtX - w, txtY, txtX + w, txtY + txtH)
  }

  override def minHeight: Int = 10

  override def computeHeight(g: Graphics2D, width: Int): Int = 10
}
