package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.isSpectatorPercentile
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.HeatmapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale

import java.awt.Color
import java.awt.Graphics2D

case class BasicHeatMap(
  graphDef: GraphDef,
  plot: PlotDef,
  axis: ValueAxis,
  timeAxis: TimeAxis,
  x1: Int,
  y1: Int,
  x2: Int,
  chartEnd: Int,
  leftOffset: Int,
  rightOffset: Int,
  query: String
) extends HeatMap {

  val yticks = axis.ticks(y1, chartEnd)

  val buckets =
    new Array[Array[Double]](yticks.size + 1) // plus 1 for the data above the final tick
  def counts: Array[Array[Double]] = buckets
  val xti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  val hCells = xti.size + 1

  var cmin = Double.MaxValue
  var cmax = Double.MinValue
  var l: Double = 0
  var u: Double = 0

  def enforceBounds: Unit = {
    l = plot.heatmapDef.getOrElse(HeatmapDef()).lower.lower(false, cmin)
    u = plot.heatmapDef.getOrElse(HeatmapDef()).upper.upper(false, cmax)
    if (l > cmin || u < cmax) {
      buckets.foreach { row =>
        for (i <- 0 until row.length) {
          val count = row(i)
          if (count < l || count > u) {
            row(i) = 0
          }
        }
      }
    }
  }

  var firstLine: LineDef = null

  def palette = firstLine.palette.getOrElse(
    Palette.singleColor(firstLine.color)
  )

  lazy val colorScaler = {
    // TODO - fix scale
    if (u < 1) {
      // only linear really makes sense here.
      Scales.linear(l, u, 0, palette.uniqueColors.size - 1)
    } else {
      plot.heatmapDef.getOrElse(HeatmapDef()).scale match {
        case Scale.LINEAR      => Scales.linear(l, u + 1, 0, palette.uniqueColors.size)
        case Scale.LOGARITHMIC => Scales.logarithmic(l, u + 1, 0, palette.uniqueColors.size)
        case Scale.POWER_2     => Scales.power(2)(l, u + 1, 0, palette.uniqueColors.size)
        case Scale.SQRT        => Scales.power(0.5)(l, u + 1, 0, palette.uniqueColors.size)
        case Scale.PERCENTILE  => Scales.linear(l, u + 1, 0, palette.uniqueColors.size)
      }
    }
  }

  def addLine(line: LineDef): Unit = {
    val seconds = if (isSpectatorPercentile(line)) bktSeconds(line) else -1
    if (firstLine == null) {
      firstLine = line
    }
    var t = graphDef.startTime.toEpochMilli
    val ti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset).iterator
    var lastTick = ti.next()
    var bi = 0
    while (t < graphDef.endTime.toEpochMilli) {
      val v = line.data.data(t)
      val cmp = if (seconds >= 0) seconds else v
      var y = 0
      val yi = yticks.iterator
      var found = false
      while (yi.hasNext && !found) {
        val ti = yi.next()
        if (cmp <= ti.v) {
          found = true
        } else {
          y += 1
        }
      }
      if (!found) {
        y = buckets.length - 1
      }

      val x = if (t <= lastTick.timestamp) {
        bi
      } else {
        if (ti.hasNext) lastTick = ti.next()
        bi += 1
        bi
      }

      if (x < hCells && y < buckets.length) {
        var b = buckets(y)
        if (b == null) {
          b = new Array[Double](hCells)
          buckets(y) = b
        }
        if (seconds >= 0) b(x) += v.toLong else b(x) += 1
        if (b(x) > 0 && b(x) > cmax) {
          cmax = b(x)
        }
        if (b(x) > 0 && b(x) < cmin) {
          cmin = b(x)
        }
      } else {
        System.out.println(
          s"************** WTF? X ${x} vs ${hCells}, Y ${y} vs ${buckets.length} @ ${t}"
        )
      }
      t += graphDef.step
    }
  }

  def draw(g: Graphics2D): Unit = {
    enforceBounds
    System.out.println("***** Draw heatmap")
    val yScaler = axis.scale(y1, chartEnd)
    val yi = yticks.iterator
    var lastY = chartEnd + 1
    buckets.foreach { bucket =>
      val ytick = if (yi.hasNext) yi.next() else null
      val nextY = if (ytick != null) yScaler(ytick.v) else y1
      if (bucket != null) {
        val lineElement = HeatmapLine(bucket, timeAxis, this)
        lineElement.draw(g, x1 + leftOffset, nextY, x2 - rightOffset, lastY - nextY)
      }
      lastY = nextY
    }
  }

  override def `type`: String = "heatmap"
}
