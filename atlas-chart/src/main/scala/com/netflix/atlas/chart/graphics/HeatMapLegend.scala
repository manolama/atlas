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
    val palette = state.firstLine.palette.getOrElse(
      Palette.singleColor(state.firstLine.color)
    )
//    val colorScaler =
//      Scales.factory(Scale.LOGARITHMIC)(state.cmin, state.cmax, 0, palette.uniqueColors.size - 1)

    val d = ChartSettings.normalFontDims.height - 2

    val colorsAndMinMax = palette.uniqueColors
      .zip(state.legendMinMax)
      // get rid of colors that weren't used.
      .filterNot(t => t._2._1 == Long.MaxValue && t._2._2 == Long.MinValue)
      .reverse

    val labelBuilder = List.newBuilder[Text]
    var maxWidth = 0

    colorsAndMinMax.foreach { t =>
      val str = UnitPrefix.format(t._2._1, "%.0f%s")
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
    val str = UnitPrefix.format(state.cmax, "%.0f%s")
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
    blockX += 3
    txt = Text(
      query,
      font = ChartSettings.normalFont,
      alignment = TextAlignment.LEFT
    )

    val txtY = y1
    val txtH = ChartSettings.normalFontDims.height
    txt.draw(g, blockX, txtY, x2, txtY + txtH)

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

  override def computeHeight(g: Graphics2D, width: Int): Int =
    ChartSettings.normalFontDims.height + ChartSettings.smallFontDims.height + 2
}
