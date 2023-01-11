package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.util.UnitPrefix

import java.awt.Font
import java.awt.Graphics2D

case class HeatMapLegend(
  styles: Styles,
  plot: PlotDef,
  plotId: Int,
  showStats: Boolean,
  graph: TimeSeriesGraph
) extends Element
    with VariableHeight {

  private val state = graph.heatmaps(plotId).getOrElse(null)
  private val (format, maxWidth) = findStringFormat(state.colorMap)
  private val blockWidth = maxWidth
  private val blockHeight = ChartSettings.normalFontDims.height - 2

  private val legendColors: List[List[HeatMapLegendColor]] = {
    if (state == null) List.empty[List[HeatMapLegendColor]]
    else {
      // see if we need to split the rows.
      val colorMap = state.colorMap
      if (((colorMap.size + 1) * blockWidth) + 2 >= graph.graphDef.width / 2) {
        val colorsPerLine = (graph.graphDef.width / 2) / maxWidth
        colorMap.grouped(colorsPerLine).toList
      } else {
        List(colorMap)
      }
    }
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    var blockY = y1
    val halfMax = Math.round(maxWidth / 2.0).toInt
    val txtH = ChartSettings.smallFontDims.height
    legendColors.foreach { colors =>
      var blockX = x1 + 2 + halfMax
      val txtY = blockY + blockHeight + 5

      colors.foreach { lc =>
        Style(lc.color).configure(g)
        g.fillRect(blockX, blockY, blockWidth, blockHeight)

        val str = UnitPrefix.format(lc.min, format)
        val txt = Text(
          str,
          font = ChartSettings.smallFont,
          alignment = TextAlignment.CENTER
        )
        txt.draw(g, blockX - halfMax, txtY, blockX + halfMax, txtY)
        blockX += blockWidth
      }

      // final tick
      val str = UnitPrefix.format(colors.last.max, format)
      val txt = Text(
        str,
        font = ChartSettings.smallFont,
        alignment = TextAlignment.CENTER
      )
      txt.draw(g, blockX - halfMax, txtY, blockX + halfMax, txtY + txtH)

      // horizontal black line
      styles.line.configure(g)
      val lineY = blockY + blockHeight
      g.drawLine(x1 + 2 + halfMax, lineY, blockX - 1, lineY)

      // vertical ticks
      var vx = x1 + 2 + halfMax
      for (i <- 0 to colors.size) {
        if (i == colors.size) {
          vx -= 1
        }
        g.drawLine(vx, lineY, vx, lineY + 3)
        vx += blockWidth
      }

      blockY =
        blockY + ChartSettings.normalFontDims.height + ChartSettings.smallFontDims.height + 2 + 5
    }

    if (state.legendLabel.length > 0) {
      styles.text.configure(g)
      val x = ((legendColors.head.size + 1) * blockWidth) + 2

      // TODO - could get fancy and figure out wraps and height, particularly if
      // the color scale wraps. Then truncate there. For now, just truncate on the
      // one line.
      val txt = Text(
        state.legendLabel,
        font = ChartSettings.normalFont,
        alignment = TextAlignment.LEFT
      ).truncate(x2 - x)

      val txtY = y1
      val txtH = ChartSettings.normalFontDims.height
      txt.draw(g, x, txtY, x2, txtY + txtH)
    }
  }

  override def minHeight: Int = if (graph.heatmaps(plotId).isEmpty) 0
  else if (graph.heatmaps(plotId).get.colorMap.isEmpty) 0
  else 10

  override def computeHeight(g: Graphics2D, width: Int): Int = {
    val height = ChartSettings.normalFontDims.height + ChartSettings.smallFontDims.height + 2
    if (legendColors.size == 1) height
    else height * legendColors.size + ((legendColors.size - 1) * 5)
  }

  private def findStringFormat(colorMap: List[HeatMapLegendColor]): (String, Int) = {
    var format = "%.0f%s"
    var maxWidth = 0
    if (colorMap.nonEmpty) {
      var lastLabel = UnitPrefix.format(colorMap.head.min, format)
      maxWidth = textWidth(lastLabel)

      var i = 1
      while (i < colorMap.size) {
        val min = colorMap(i).min
        val nextLabel = UnitPrefix.format(min, format)
        if (lastLabel.equals(nextLabel) && format.equals("%.0f%s")) {
          format = "%.1f%s"
          lastLabel = UnitPrefix.format(colorMap.head.min, format)
          maxWidth = textWidth(lastLabel)
          i = 1
        }
        lastLabel = nextLabel

        val str = UnitPrefix.format(min, format)
        val width = textWidth(str)
        if (width > maxWidth) {
          maxWidth = width
        }

        i += 1
      }

      val str = UnitPrefix.format(colorMap.last.max, format)
      val width = textWidth(str)
      if (width > maxWidth) {
        maxWidth = width
      }
    }

    (format, maxWidth)
  }

  private def textWidth(string: String, font: Font = ChartSettings.smallFont): Int = {
    ChartSettings.refGraphics.getFontMetrics(font).stringWidth(s" ${string} ")
  }
}
