package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.model.DataDef
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.DatapointMeta

import java.awt.Graphics2D

case class MetaLegendEntry(
  graphDef: GraphDef,
  styles: Styles,
  plot: PlotDef,
  data: DataDef,
  graph: TimeSeriesGraph
) extends Element
    with FixedHeight {

  val text = {
    val leftAxisW = graph.yaxes.head.width
    val rightAxisW = graph.yaxes.tail.foldLeft(0) { (acc, axis) =>
      acc + axis.width
    }
    val rightSideW = if (rightAxisW > 0) rightAxisW else TimeSeriesGraph.minRightSidePadding
    val axisW = leftAxisW + rightSideW
    val width = graph.width - axisW // x2 - x1 - axisW
    val showAxes = !graphDef.onlyGraph && width >= GraphConstants.MinCanvasWidth
    val leftOffset = if (showAxes) leftAxisW else TimeSeriesGraph.minRightSidePadding
    val rightOffset = if (showAxes) rightSideW else TimeSeriesGraph.minRightSidePadding

    val list = List.newBuilder[Text]
    val ticks =
      graph.timeAxis
        .ticks(leftOffset, graph.width - rightOffset)
        .filter(_.major)
    for (i <- graphDef.startTime.toEpochMilli until graphDef.endTime.toEpochMilli) {
      data match {
        case line: LineDef =>
          line.data.datapointMeta(i).map { meta =>
            ticks.find(_.timestamp == i).map { _ =>
              list += Text(
                s"$i) ${formatMeta(meta)}",
                font = ChartSettings.smallFont,
                alignment = TextAlignment.LEFT,
                style = styles.text
              )
            }
          }
        case _ =>
      }
    }
    list.result()
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {

    val d = ChartSettings.normalFontDims.height - 4

    // Draw the color box for the legend entry. If the color has an alpha setting, then the
    // background can impact the color so we first fill with the background color of the chart.
    g.setColor(styles.background.color)
    g.fillRect(x1 + 2, y1 + 2, d, d)
    g.setColor(data.color)
    g.fillRect(x1 + 2, y1 + 2, d, d)

    // Border for the color box
    styles.line.configure(g)
    g.drawRect(x1 + 2, y1 + 2, d, d)

    // Draw the label
    val txt = Text(data.label, alignment = TextAlignment.LEFT, style = styles.text)
    val truncated = txt.truncate(x2 - x1 - d - 4)
    truncated.draw(g, x1 + d + 4, y1, x2, y2)

    val offset = y1 + ChartSettings.normalFontDims.height
    val rowHeight = ChartSettings.smallFontDims.height

    text.zipWithIndex.foreach {
      case (txt, i) =>
        txt.truncate(x2 - x1 - d - 4).draw(g, x1 + d + 4, offset + i * rowHeight, x2, y2)
    }
//    for (i <- graphDef.startTime.toEpochMilli until graphDef.endTime.toEpochMilli) {
//      data match {
//        case line: LineDef =>
//          line.data.datapointMeta(i).map { meta =>
//            val txt = text(i.toInt).truncate(x2 - x1 - d - 4)
//            txt.draw(g, x1 + d + 4, offset + i.toInt * rowHeight, x2, y2)
//          }
//        case _ =>
//      }
//    }
  }

  def formatMeta(meta: DatapointMeta): String = {
    meta.keys.map { case k => s"$k=${meta.get(k).getOrElse("")}" }.mkString(", ")
  }

  override def height: Int = {
    ChartSettings.normalFontDims.height +
      (ChartSettings.smallFontDims.height * text.size)
  }
}
