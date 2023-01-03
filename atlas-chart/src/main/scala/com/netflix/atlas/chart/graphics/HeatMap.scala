package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.isSpectatorPercentile
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale

import java.awt.Graphics2D

case class HeatMap(
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
) extends HeatMapState {

  System.out.println("***** Start heatmap")
  val yticks = axis.ticks(y1, chartEnd)

  val buckets =
    new Array[Array[Long]](yticks.size + 1) // plus 1 for the data above the final tick
  val xti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  val hCells = xti.size + 1

  var cmin = Long.MaxValue
  var cmax = Long.MinValue
  var firstLine: LineDef = null
  var legendMinMax: Array[(Long, Long)] = null

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
          b = new Array[Long](hCells)
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
    System.out.println("***** Draw heatmap")
    val palette = firstLine.palette.getOrElse(
      Palette.singleColor(firstLine.color)
    )
    legendMinMax = new Array[(Long, Long)](palette.uniqueColors.size)
    for (i <- 0 until legendMinMax.length) legendMinMax(i) = (Long.MaxValue, Long.MinValue)
    val colorScaler =
      Scales.factory(Scale.LOGARITHMIC)(cmin, cmax, 0, palette.uniqueColors.size - 1)
    val yScaler = axis.scale(y1, chartEnd)
    val yi = yticks.iterator
    var lastY = chartEnd + 1
    buckets.foreach { bucket =>
      val ytick = if (yi.hasNext) yi.next() else null
      val nextY = if (ytick != null) yScaler(ytick.v) else y1
      if (bucket != null) {
        val lineElement = HeatmapLine(bucket, timeAxis, colorScaler, palette, legendMinMax)
        lineElement.draw(g, x1 + leftOffset, nextY, x2 - rightOffset, lastY - nextY)
      }
      lastY = nextY
    }
  }

  override def `type`: String = "heatmap"
}
