package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.MessageDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.spectator.api.histogram.PercentileBuckets

import java.awt.BasicStroke
import java.awt.Color
import java.awt.Graphics2D
import scala.collection.mutable

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
      if (i == 0) {
        // TODO - need log support
        val minSecs = bktNanos(plot.lines.head).toDouble / 1000 / 1000 / 1000
        val maxSecs = bktNanos(plot.lines.last).toDouble / 1000 / 1000 / 1000
        System.out.println(s"Y Min sec ${minSecs}  Max: ${maxSecs}")
        LeftValueAxis(plot, graphDef.theme.axis, minSecs, maxSecs)
        // LeftValueAxis(plot, graphDef.theme.axis, bounds._1, bounds._2)
      } else
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
    val plot = graphDef.plots(0)
    val bktRange = bktIdx(plot.lines.last) - bktIdx(plot.lines.head)
    val minNanos = bktNanos(plot.lines.head)
    val maxNanos = bktNanos(plot.lines.last)
    val ypixels = y2 - y1 - timeAxisH
    val lines = plot.lines.length
    val dpHeight = ypixels / bktRange
    System.out.println(
      s"Min Nanos ${minNanos}  Max Nanos ${maxNanos}  Pixes: ${ypixels}  DPH: ${dpHeight} Series ${plot.data.length} Bkt Range: ${bktRange}"
    )
    val logScaler = Scales.factory(Scale.LOGARITHMIC)(minNanos, maxNanos, 0, ypixels / dpHeight)

    // val combinedSeries = new Array[Array[Long]](Math.min(ypixels, lines))
    val combinedSeries = new mutable.TreeMap[Int, (Long, Array[Double])]
    val numDps = (graphDef.endTime.toEpochMilli - graphDef.startTime.toEpochMilli) / graphDef.step
    val reds = Palette.fromResource("reds")
    var redList = List.empty[Color]
    for (i <- 0 until 7) {
      redList ::= reds.colors(i)
    }

    var cmin = Long.MaxValue
    var cmax = Long.MinValue
    val temp = new mutable.TreeMap[Int, Int]()
    plot.lines.foreach { line =>
      // val idx = (ypixels / dpHeight) - logScaler(bktNanos(line))
      val idx = bktIdx(line) * dpHeight
      //yoff += dpHeight
      // System.out.println(s"  IDX: ${idx} vs Log ${logScaler(bktNanos(line))} vs ${yoff}")
      // System.out.println(s"  idx: ${idx}  Nanos: ${bktNanos(line)}")
      val cnt = temp.getOrElseUpdate(idx, 0)
      temp += (idx -> (cnt + 1))
      //      if (combinedSeries(idx) == null) {
      //        combinedSeries(idx) = new Array[Double](numDps.toInt)
      //      }
      val (nanos, arr) =
        combinedSeries.getOrElseUpdate(idx, bktNanos(line) -> new Array[Double](numDps.toInt))
      var t = graphDef.startTime.toEpochMilli
      var di = 0
      while (t < graphDef.endTime.toEpochMilli) {
        val v = (line.data.data(t) * 60).toLong
        arr(di) += v
        if (arr(di) > cmax) cmax = arr(di).toLong
        if (arr(di) < cmin) cmin = arr(di).toLong
        di += 1
        t += line.data.data.step
      }
    }
    // temp.foreachEntry { (k, v) => System.out.println(s"${k}: ${v}") }
    val colorScaler = Scales.factory(Scale.LOGARITHMIC)(cmin, cmax, 0, redList.size - 1)

    val step =
      System.out.println(s"Max counts: ${cmax}  Min counts: ${cmin}  Y Pixels: ${ypixels}")

    graphDef.plots.zip(yaxes).foreach {
      case (plot, axis) =>
        // TODO - if we have to consolidate buckets, do that
        System.out
          .println(s"Plot: ${plot.lines.length}  Axis: ${axis.valueScale}  DPH: ${dpHeight}")

        combinedSeries.foreachEntry { (idx, tuple) =>
          val (nanos, line) = tuple
          val ts = new ArrayTimeSeq(DsType.Gauge, graphDef.startTime.toEpochMilli, 60_000, line)
          val style = Style(color = Color.RED, stroke = new BasicStroke(1))
          // val i = (ypixels - (idx * dpHeight))
          // val i = ypixels - idx
          val i = ypixels - idx
          System.out.println(s"  OFF: ${i}  nanos ${nanos}")
          val lineElement =
            PTileHistoLine(
              style,
              ts,
              timeAxis,
              axis,
              cmax - cmin,
              i,
              dpHeight,
              colorScaler,
              redList
            )
          lineElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
        }
      // val offsets = TimeSeriesStack.Offsets(timeAxis)
//
//        plot.lines.foreach { line =>
//          val idx = ypixels - logScaler(bktNanos(line))
//          // System.out.println(s"   Log y: ${idx} vs Y: ${yOff} ")
//          val bktIdx = Integer.parseInt(line.data.tags("percentile").substring(1), 16)
//          val nanos = PercentileBuckets.get(bktIdx)
//
//          val style = Style(color = line.color, stroke = new BasicStroke(line.lineWidth))
//          val lineElement =
//            PTileHistoLine(style, line.data.data, timeAxis, axis, cmax - cmin, yOff, dpHeight)
//          lineElement.draw(g, x1 + leftOffset, y1, x2 - rightOffset, chartEnd)
//          yOff += dpHeight
//        }
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
    PercentileBuckets.get(bktIdx(line))
  }

  def bktIdx(line: LineDef): Int = {
    Integer.parseInt(line.data.tags("percentile").substring(1), 16)
  }

}
