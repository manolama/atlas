package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.util.UnitPrefix

import java.awt.Color
import java.awt.Graphics2D

case class HeatMapLegend(
  styles: Styles,
  plot: PlotDef,
  plotId: Int,
  showStats: Boolean,
  query: String,
  graph: TimeSeriesGraph
) extends Element
    with VariableHeight {

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val state = graph.heatmaps(plotId)
//    val palette = state.firstLine.palette.getOrElse(
//      Palette.singleColor(state.firstLine.color)
//    )
    val palette = state.palette

    // TODO - e.g. using 24 colors, the legend goes off the canvas. Need to handle that.
    // even throws an exception printing the last text box.
    val d = ChartSettings.normalFontDims.height - 2

    val colorsAndMinMax = palette.uniqueColors
      .zip(state.legendMinMax)
      // get rid of colors that weren't used.
      .filterNot(t => t._2._3 == 0)
      .reverse

    val labelBuilder = List.newBuilder[Text]
    var maxWidth = 0

    val format =
      // if (colorsAndMinMax.last._2._1 - colorsAndMinMax.last._2._1.toLong > 0) "%.1f%s" else "%.0f%s"
      "%.0f%s"
    colorsAndMinMax.foreach { t =>
      val str = UnitPrefix.format(t._2._1, format)
      val txt = Text(
        str,
        font = ChartSettings.smallFont,
        alignment = TextAlignment.CENTER
      )

      // val width = str.length * txt.dims.width
      val width = g.getFontMetrics.stringWidth(str)
      if (width > maxWidth) {
        maxWidth = width
      }
      labelBuilder += txt
    }

    // max
    val str = UnitPrefix.format(state.u, format)
    var txt = Text(
      str,
      font = ChartSettings.smallFont,
      alignment = TextAlignment.CENTER
    )
    val width = str.length * txt.dims.width
    if (width > maxWidth) {
      maxWidth = width
    }
    labelBuilder += txt

    val w = Math.max(d, maxWidth) + 1
    val halfMax = w / 2
    var blockX = x1 + 2 + halfMax
    val blockY = y1
    val labels = labelBuilder.result()
    colorsAndMinMax.zip(labels).foreach { t =>
      Style(t._1._1).configure(g)
      g.fillRect(blockX, blockY, w, d)

      val text = t._2
      val txtH = ChartSettings.smallFontDims.height
      val txtY = blockY + d + 5
      text.draw(g, blockX - halfMax, txtY, blockX + halfMax, txtY + txtH)

      blockX += w
    }

    {
      val text = labels.last
      val txtH = ChartSettings.smallFontDims.height
      val txtY = blockY + d + 5
      text.draw(g, blockX - halfMax, txtY, blockX + halfMax, txtY + txtH)
    }

    // horizontal black line
    styles.line.configure(g)
    val lineY = y1 + d
    g.drawLine(x1 + 2 + halfMax, lineY, blockX - 1, lineY)

    // TICKS
    var vx = x1 + 2 + halfMax
    for (i <- 0 to colorsAndMinMax.size) {
      if (i == colorsAndMinMax.size) {
        vx -= 1
      }
      g.drawLine(vx, lineY, vx, lineY + 3)
      vx += w
    }

    styles.text.configure(g)
    blockX += 5 // TODO - compute
    txt = Text(
      query,
      font = ChartSettings.normalFont,
      alignment = TextAlignment.LEFT
    )

    val txtY = y1
    val txtH = ChartSettings.normalFontDims.height
    txt.draw(g, blockX, txtY, x2, txtY + txtH)
  }

  override def minHeight: Int = 10

  override def computeHeight(g: Graphics2D, width: Int): Int =
    ChartSettings.normalFontDims.height + ChartSettings.smallFontDims.height + 2
}
