package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.MessageDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.spectator.api.histogram.PercentileBuckets

import java.awt.BasicStroke
import java.awt.Graphics2D

case class PercentileHeatMap(graphDef: GraphDef) extends Element with FixedHeight with FixedWidth {

  val start: Long = graphDef.startTime.toEpochMilli
  val end: Long = graphDef.endTime.toEpochMilli

  override def height: Int = {
    val max = GraphConstants.MaxHeight
    val h =
      if (graphDef.height > max) max
      else {
        val min = GraphConstants.MinCanvasHeight
        if (graphDef.height < min) min else graphDef.height
      }
    if (graphDef.onlyGraph || graphDef.layout.isFixedHeight) h
    else {
      h + timeAxes.map(_.height).sum
    }
  }

  override def width: Int = {
    val max = GraphConstants.MaxWidth
    val w =
      if (graphDef.width > max) max
      else {
        val min = GraphConstants.MinCanvasWidth
        if (graphDef.width < min) min else graphDef.width
      }
    if (graphDef.onlyGraph || graphDef.layout.isFixedWidth) w
    else {
      val rightPadding = if (yaxes.tail.nonEmpty) 0 else TimeSeriesGraph.minRightSidePadding
      w + yaxes.map(_.width).sum + rightPadding
    }
  }

  val timeAxes: List[TimeAxis] = graphDef.timezones.zipWithIndex.map {
    case (tz, i) =>
      TimeAxis(
        Style(color = graphDef.theme.axis.line.color),
        start,
        end,
        graphDef.step,
        tz,
        if (i == 0) 40 else 0xFF
      )
  }
  val timeAxis: TimeAxis = timeAxes.head

  val yaxes: List[ValueAxis] = graphDef.plots.zipWithIndex.map {
    case (plot, i) =>
      val bounds = plot.bounds(start, end)
      if (i == 0)
        LeftValueAxis(plot, graphDef.theme.axis, bounds._1, bounds._2)
      else
        RightValueAxis(plot, graphDef.theme.axis, bounds._1, bounds._2)
  }

  private def clip(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    g.setClip(x1, y1, x2 - x1, y2 - y1)
    g.setColor(graphDef.theme.canvas.background.color)
    g.fillRect(x1, y1, x2 - x1, y2 - y1)
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val leftAxisW = yaxes.head.width
    val rightAxisW = yaxes.tail.foldLeft(0) { (acc, axis) =>
      acc + axis.width
    }
    val rightSideW = if (rightAxisW > 0) rightAxisW else TimeSeriesGraph.minRightSidePadding
    val axisW = leftAxisW + rightSideW
    val width = x2 - x1 - axisW

    val showAxes = !graphDef.onlyGraph && width >= GraphConstants.MinCanvasWidth
    val leftOffset = if (showAxes) leftAxisW else TimeSeriesGraph.minRightSidePadding
    val rightOffset = if (showAxes) rightSideW else TimeSeriesGraph.minRightSidePadding

    val timeAxisH = if (graphDef.onlyGraph) 10 else timeAxis.height
    val timeGrid = TimeGrid(timeAxis, graphDef.theme.majorGrid.line, graphDef.theme.minorGrid.line)

    val chartEnd = y2 - timeAxisH * timeAxes.size

    val prevClip = g.getClip
    clip(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd + 1)

    // resort
    val logScaler = Scales.factory(Scale.LOGARITHMIC)
    val plot = graphDef.plots(0)
    val minNanos = bktNanos(plot.lines.head)
    val maxNanos = bktNanos(plot.lines.last)
    val ypixels = y2 - y1
    val lines = plot.lines.length
    val dpHeight =
      if (lines >= ypixels) 1
      else ypixels / lines
    

    var cmin = Double.MaxValue
    var cmax = Double.MinValue
    plot.lines.foreach { d =>
      if (d.isInstanceOf[MessageDef]) {
        System.out.println(s"Whoops, msg: ${d}")
      } else if (d.isInstanceOf[LineDef]) {
        val lineDef = d.asInstanceOf[LineDef]
        val x = lineDef.legendStats.max
        if (x > cmax) {
          cmax = x
        }
        val n = lineDef.legendStats.min
        if (n < cmin) {
          cmin = n
        }
      }
    }

    System.out.println(s"Max counts: ${cmax}  Min counts: ${cmin}  Y Pixels: ${ypixels}")

    graphDef.plots.zip(yaxes).foreach {
      case (plot, axis) =>
        // TODO - if we have to consolidate buckets, do that
        System.out
          .println(s"Plot: ${plot.lines.length}  Axis: ${axis.valueScale}  DPH: ${dpHeight}")

        // val offsets = TimeSeriesStack.Offsets(timeAxis)
        var yOff = 0
        plot.lines.foreach { line =>
          val bktIdx = Integer.parseInt(line.data.tags("percentile").substring(1), 16)
          val nanos = PercentileBuckets.get(bktIdx)

          val style = Style(color = line.color, stroke = new BasicStroke(line.lineWidth))
          val lineElement =
            PTileHistoLine(style, line.data.data, timeAxis, axis, cmax - cmin, yOff, dpHeight)
//          val style = Style(color = line.color, stroke = new BasicStroke(line.lineWidth))
          //          val lineElement = line.lineStyle match {
          //            case LineStyle.LINE  => TimeSeriesLine(style, line.data.data, timeAxis, axis)
          //            case LineStyle.AREA  => TimeSeriesArea(style, line.data.data, timeAxis, axis)
          //            case LineStyle.VSPAN => TimeSeriesSpan(style, line.data.data, timeAxis)
          //            case LineStyle.STACK => TimeSeriesStack(style, line.data.data, timeAxis, axis, offsets)
          //          }
          //
          lineElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
          yOff += dpHeight
        }
    }
    g.setClip(prevClip)

    timeGrid.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)

    if (!graphDef.onlyGraph) {
      timeAxes.zipWithIndex.foreach {
        case (axis, i) =>
          val offset = chartEnd + 1 + timeAxisH * i
          axis.draw(g, x1 + leftOffset, offset, x2 - rightOffset, y2)
      }
    }

    val valueGrid =
      ValueGrid(yaxes.head, graphDef.theme.majorGrid.line, graphDef.theme.minorGrid.line)
    valueGrid.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
    if (showAxes) {
      yaxes.head.draw(g, x1, y1, x1 + leftAxisW - 1, chartEnd)
      yaxes.tail.zipWithIndex.foreach {
        case (axis, i) =>
          val offset = leftAxisW + width + leftAxisW * i
          axis.draw(g, x1 + offset, y1, x1 + offset + leftAxisW, chartEnd)
      }
    }
  }

  def bktNanos(line: LineDef): Long = {
    val bktIdx = Integer.parseInt(line.data.tags("percentile").substring(1), 16)
    PercentileBuckets.get(bktIdx)
  }

}
