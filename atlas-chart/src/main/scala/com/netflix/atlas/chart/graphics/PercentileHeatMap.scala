package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktNanos
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.isSpectatorPercentile
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.MessageDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.spectator.api.histogram.PercentileBuckets

import java.awt.BasicStroke
import java.awt.Color
import java.awt.Graphics2D
import java.util.regex.Pattern
import scala.collection.mutable

case class PercentileHeatMap(
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
    val bktIdx = PercentileHeatMap.bktIdx(line)
    val prevBktIdx = if (bktIdx == 0) 0 else bktIdx - 1
    val prevBktSec = bktSeconds(prevBktIdx)
    // we need to figure out how many buckets to fill. This is due to the regular
    // ticks scale but exponential bucket size. Higher buckets can spread across
    // cells.
    var bucketsPerBucket = -1
    val yti = yticks.iterator
    var y = 0
    var f = false
    while (yti.hasNext && !f) {
      val ti = yti.next()
      if (ti.v > prevBktSec) {
        bucketsPerBucket += 1
      }
      if (ti.v > seconds) {
        f = true
      } else {
        y += 1
      }
    }
    if (!f) {
      y = buckets.length - 1
    }

    if (firstLine == null) {
      firstLine = line
    }
    var t = graphDef.startTime.toEpochMilli
    val ti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset).iterator
    var lastTick = ti.next()
    var bi = 0
    while (t < graphDef.endTime.toEpochMilli) {
      val v = line.data.data(t)
//      val cmp = if (seconds >= 0) seconds else v
//      var y = 0
//      val yi = yticks.iterator
//      var found = false
//      while (yi.hasNext && !found) {
//        val ti = yi.next()
//        if (cmp <= ti.v) {
//          found = true
//        } else {
//          y += 1
//        }
//      }
//      if (!found) {
//        y = buckets.length - 1
//      }

      val x = if (t <= lastTick.timestamp) {
        bi
      } else {
        if (ti.hasNext) lastTick = ti.next()
        bi += 1
        bi
      }

      val bktCount = v / bucketsPerBucket

      if (x < hCells && y < buckets.length) {
        for (i <- y - bucketsPerBucket to y) {
          var row = buckets(i)
          if (row == null) {
            row = new Array[Long](hCells)
            buckets(i) = row
          }
          if (seconds >= 0) row(x) += v.toLong else row(x) += 1
          if (row(x) > 0 && row(x) > cmax) {
            cmax = row(x)
          }
          if (row(x) > 0 && row(x) < cmin) {
            cmin = row(x)
          }
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
}

object PercentileHeatMap {

  private val timerBucketIdPattern = Pattern.compile("^T[0-9A-F]{4}$")

  def isSpectatorPercentile(line: LineDef): Boolean = {
    line.data.tags.get("percentile") match {
      case Some(v) => timerBucketIdPattern.matcher(v).find()
      case _       => false
    }
  }

  def bktNanos(line: LineDef): Long = {
    PercentileBuckets.get(bktIdx(line))
  }

  def bktSeconds(line: LineDef): Double = {
    PercentileBuckets.get(bktIdx(line)) / 1000.0 / 1000.0 / 1000.0
  }

  def bktSeconds(bkt: Int): Double = {
    PercentileBuckets.get(bkt) / 1000.0 / 1000.0 / 1000.0
  }

  def bktIdx(line: LineDef): Int = {
    Integer.parseInt(line.data.tags("percentile").substring(1), 16)
  }
}
