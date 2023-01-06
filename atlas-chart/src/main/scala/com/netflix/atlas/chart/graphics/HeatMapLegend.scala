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
    val blockHeight = ChartSettings.normalFontDims.height - 2

//    val colorsAndMinMax = if (state.legendMinMax == null) {
//      List.empty[(Color, (Double, Double, Long))]
//    } else {
//      palette.uniqueColors.reverse
//        .zip(state.legendMinMax)
//        // get rid of colors that weren't used.
//        .filterNot(t => t._2._3 == 0)
//      // .reverse
//    }
    val legendColors = state.colorMap

    val labelBuilder = List.newBuilder[Text]
    var maxWidth = 0

    // TODO - check for uniques!
    var format = "%.0f%s"
    if (legendColors.nonEmpty) {
      var lastLabel = UnitPrefix.format(legendColors.head.min, format)
      for (i <- 1 until legendColors.size) {
        val nextLabel = UnitPrefix.format(legendColors(i).min, format)
        // System.out.println(s"next ${nextLabel} prev ${lastLabel}")
        if (lastLabel.equals(nextLabel)) {
          format = "%.1f%s"
        }
        lastLabel = nextLabel
      }

      legendColors.foreach { t =>
        val str = UnitPrefix.format(t.min, format)
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
    }

    // max
    val last = if (legendColors.nonEmpty) {
      legendColors.last.max
    } else {
      0
    }
    val str = UnitPrefix.format(last, format)
    var txt = Text(
      str,
      font = ChartSettings.smallFont,
      alignment = TextAlignment.CENTER
    )
    val width = g.getFontMetrics.stringWidth(str)
    if (width > maxWidth) {
      maxWidth = width
    }
    labelBuilder += txt

    val w = maxWidth + 2
    val halfMax = w / 2
    var blockX = x1 + 2 + halfMax
    val blockY = y1
    val labels = labelBuilder.result()
    legendColors.zip(labels).foreach { t =>
      val (legend, label) = t
      Style(legend.color).configure(g)
      g.fillRect(blockX, blockY, w, blockHeight)

      val text = label
      val txtH = ChartSettings.smallFontDims.height
      val txtY = blockY + blockHeight + 5
      text.draw(g, blockX - halfMax, txtY, blockX + halfMax, txtY + txtH)

      blockX += w
    }

    if (legendColors.nonEmpty) {
      {
        val text = labels.last
        val txtH = ChartSettings.smallFontDims.height
        val txtY = blockY + blockHeight + 5
        text.draw(g, blockX - halfMax, txtY, blockX + halfMax, txtY + txtH)
      }

      // horizontal black line
      styles.line.configure(g)
      val lineY = y1 + blockHeight
      g.drawLine(x1 + 2 + halfMax, lineY, blockX - 1, lineY)

      // TICKS
      var vx = x1 + 2 + halfMax
      for (i <- 0 to legendColors.size) {
        if (i == legendColors.size) {
          vx -= 1
        }
        g.drawLine(vx, lineY, vx, lineY + 3)
        vx += w
      }
    }

    if (query.length > 0) {
      styles.text.configure(g)
      blockX += halfMax
      txt = Text(
        query,
        font = ChartSettings.normalFont,
        alignment = TextAlignment.LEFT
      )

      val txtY = y1
      val txtH = ChartSettings.normalFontDims.height
      txt.draw(g, blockX, txtY, x2, txtY + txtH)
    }
  }

  override def minHeight: Int = 10

  override def computeHeight(g: Graphics2D, width: Int): Int =
    ChartSettings.normalFontDims.height + ChartSettings.smallFontDims.height + 2
}
