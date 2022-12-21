package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktNanos
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.getScale
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
  val yscale = axis.scale(y1, chartEnd)
  val scale = getScale(axis.min, axis.max, y1, chartEnd)

  case class Bkt(
    counts: Array[Long],
    offset: Int,
    height: Int,
    tick: ValueTick,
    ptileScale: List[PtileScale],
    ticksPerBucket: Int
  )

  val buckets = {
    val bkts = new Array[Bkt](yticks.size)

    // two ways to combine.
    // 1) There are more buckets than ticks.
    // 2) There are more ticks than buckets.
    var idx = 0
    if (yticks.length > scale.length) {
      // splits
      scale.foreach { s =>
        var last = yscale(s.prevBoundary)
        val filtered = yticks.filter(t => t.v > s.prevBoundary && t.v <= s.boundary)
        filtered.foreach { tick =>
          val offset = yscale(tick.v)
          bkts(idx) = Bkt(null, offset, last - offset, tick, List(s), filtered.size)
          last = offset
          idx += 1
        }
      }
    } else {
      // consolidate possibly
      var last = chartEnd
      yticks.foreach { tick =>
//        val filtered = scale.filter(s => s.boundary > last && s.boundary <= tick.v)
//        if (filtered.nonEmpty) {
        val offset = yscale(tick.v)
        bkts(idx) = Bkt(null, offset, last - offset, tick, List.empty, 1)
        last = offset
//        } else {
//          bkts(idx) = Bkt(null, 0, 0, tick, filtered, 1)
//        }
        idx += 1
      }
    }

    bkts
  }
  val xti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  val hCells = xti.size + 1

  var cmin = Long.MaxValue
  var cmax = Long.MinValue
  var firstLine: LineDef = null
  var legendMinMax: Array[(Long, Long)] = null

  def addLine(line: LineDef): Unit = {
    val seconds = if (isSpectatorPercentile(line)) bktSeconds(line) else -1
    // we need to figure out how many buckets to fill. This is due to the regular
    // ticks scale but exponential bucket size. Higher buckets can spread across
    // cells.
    var (row, idx) = buckets.zipWithIndex
      .find(bkt => seconds <= bkt._1.tick.v)
      .getOrElse((buckets.head, 0))

    System.out.println(s"*** Row seconds ${seconds} in bkt Boundary ${row.tick.v}")
    if (firstLine == null) {
      firstLine = line
    }

    var t = graphDef.startTime.toEpochMilli
    val ti = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset).iterator
    var lastTick = ti.next()
    var bi = 0
    while (t < graphDef.endTime.toEpochMilli) {
      val v = line.data.data(t)
      val x = if (t <= lastTick.timestamp) {
        bi
      } else {
        if (ti.hasNext) lastTick = ti.next()
        bi += 1
        bi
      }

      if (v > 0) {
        if (x < hCells) {
          val tpb = row.ticksPerBucket
          // TODO - change counts to double now.... confusing as all get out....
          val count = Math.max(1, (v / tpb).toInt)
          for (i <- 0 until tpb) {
            if (idx - i >= 0) {
              row = buckets(idx - i)
              if (row.counts == null) {
                row = row.copy(counts = new Array[Long](hCells))
                buckets(idx - i) = row
              }

              if (seconds >= 0) row.counts(x) += count else row.counts(x) += 1

              if (row.counts(x) > 0 && row.counts(x) > cmax) {
                cmax = row.counts(x)
              }
              if (row.counts(x) > 0 && row.counts(x) < cmin) {
                cmin = row.counts(x)
              }
            }
          }
          //        }
        } else {
          System.out.println(
            s"************** WTF? X ${x} vs ${hCells}, Y ${idx} vs ${buckets.length} @ ${t}"
          )
        }
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
      if (bucket != null && bucket.counts != null) {
        val lineElement = HeatmapLine(bucket.counts, timeAxis, colorScaler, palette, legendMinMax)

        // val yy = (chartEnd - bucket.offset + y1 + bucket.height) - bucket.height
        val yy = bucket.offset
        System.out.println(s"YY ${yy} & H ${bucket.height}")
        lineElement.draw(
          g,
          x1 + leftOffset,
          // lastY,
          yy,
          x2 - rightOffset,
          // lastY - nextY
          bucket.height
        )
      }
      lastY = nextY
    }
  }

}

case class PtileScale(
  boundary: Double,
  offset: Int,
  height: Int,
  prevBoundary: Double,
  bktIndex: Int,
  split: Boolean
)

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

  def getScale(d1: Double, d2: Double, y1: Int, y2: Int): List[PtileScale] = {
    val (minBkt, maxBkt) = minMaxBuckets(d1, d2)
    val bktRange = maxBkt - minBkt
    val avgHeight = (y2 - y1).toDouble / (bktRange + 1)

    var ticks = List.empty[PtileScale]
    var cnt = 0
    var prev = y1.toDouble
    var prevI = if (minBkt == 0) 0.0 else bktSeconds(minBkt - 1)
    for (i <- minBkt until maxBkt) {
      val next = prev + avgHeight
      val h = (Math.round(next) - Math.round(prev)).toInt
      val offset = Math.round(prev).toInt + h
      val sec = bktSeconds(i)
      ticks = ticks :+ PtileScale(sec, offset, h, prevI, i, h >= 15)
      cnt += 1
      prevI = sec
      prev = next
    }
    ticks
  }

  def minMaxBuckets(min: Double, max: Double): (Int, Int) = {
    (
      PercentileBuckets.indexOf((min * 1000 * 1000 * 1000).toLong),
      PercentileBuckets.indexOf((max * 1000 * 1000 * 1000).toLong)
    )
  }
}
