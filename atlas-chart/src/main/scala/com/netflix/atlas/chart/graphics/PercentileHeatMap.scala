package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.GraphConstants
import com.netflix.atlas.chart.graphics.HeatMap.choosePalette
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktNanos
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.getScale
import com.netflix.atlas.chart.graphics.PercentileHeatMap.isSpectatorPercentile
import com.netflix.atlas.chart.graphics.ValueAxis.minTickLabelHeight
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.HeatmapDef
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
import java.util
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
  query: String,
  leftOffset: Int = 0,
  rightOffset: Int = 0
) extends HeatMap {

  val yticks = axis.ticks(y1, chartEnd)

  private val yscale = axis.scale(y1, chartEnd)
  private val ptileScale = getScale(axis.min, axis.max, y1, chartEnd)
  private val xTicks = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  private val hCells = xTicks.size + 1

  private var cmin = Double.MaxValue
  private var cmax = Double.MinValue
  private var lowerCellBound: Double = 0
  private var upperCellBound: Double = 0
  private var firstLine: LineDef = null

  case class Bkt(
    counts: Array[Double],
    y: Int,
    height: Int,
    v: Double,
    next: Double,
    ticksPerBucket: Int
  )

  val buckets = {
    val bkts = new Array[Bkt](yticks.size - 1)
    // simplify I hope....
    val tpm =
      if (yticks.size - 1 >= ptileScale.size) {
        Math.max(1, (yticks.size - 1) / ptileScale.size)
      } else
        1
    var lastY = y1
    for (i <- yticks.length - 2 to 0 by -1) {
      val tick = yticks(i)
      val y = yscale(tick.v)
      val nextTick = yticks(i + 1)
      bkts(i) = Bkt(null, y, y - lastY, tick.v, nextTick.v, tpm)
      lastY = y
    }

    bkts // TODO - still producing null buckets.... crap
  }

  def counts: Array[Array[Double]] = {
    val results = new Array[Array[Double]](buckets.length)
    buckets.zipWithIndex.foreach { tuple =>
      val (bkt, idx) = tuple
      if (bkt == null) {
        results(idx) = new Array[Double](hCells)
      } else {
        results(idx) = bkt.counts
      }
    }
    // TODO - still producing null buckets.... crap
    results
  }

  lazy val palette = choosePalette(firstLine)

  lazy val colorScaler = HeatMap.colorScaler(plot, palette, lowerCellBound, upperCellBound)

  def addLine(line: LineDef): Unit = {
    // Remember, when we make this call, it's actually the NEXT bucket so we need
    // to shift down.
    val seconds = if (isSpectatorPercentile(line)) {
      var b = bktIdx(line)
      b = if (b > 0) b - 1 else b
      bktSeconds(b)
    } else -1
    // bounds check
    if (seconds > yticks.last.v || seconds < yticks.head.v) {
      return
    }
    // we need to figure out how many buckets to fill. This is due to the regular
    // ticks scale but exponential bucket size. Higher buckets can spread across
    // cells.
    // var i = 0
    var bucketIndex = 0
    var found = false
    buckets.zipWithIndex.foreach { tuple =>
      val (bk, i) = tuple
      if (seconds >= bk.v && seconds < bk.next) {
        found = true
        bucketIndex = i
      }
    }
    if (!found) {
      System.out.println(s"********** WTF? Nothing found for ${seconds}????")
      bucketIndex = 0
    }
    var row = buckets(bucketIndex)
    if (bucketIndex >= buckets.length) {
      System.out.println("WTF? Out of range????")
    }
    // System.out.println(s"ROW: ${bucketIndex} for ${seconds}")

    // System.out.println(s"*** Row seconds ${seconds} in bkt Boundary ${row.tick.v}")
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

      if (v > 0) { // filters NaNs
        if (x < hCells) {
          val tpb = row.ticksPerBucket
          // TODO - change counts to double now.... confusing as all get out....
          // val count = Math.max(1, (v / tpb).toInt)
          val count = v / tpb
          for (i <- 0 until tpb) {
            if (bucketIndex + i < buckets.length) { // edge case if there is only a single bucket.
              row = buckets(bucketIndex + i)
              if (row.counts == null) {
                row = row.copy(counts = new Array[Double](hCells))
                buckets(bucketIndex + i) = row
              }

              if (seconds >= 0) row.counts(x) += count else row.counts(x) += 1

              if (row.counts(x) > 0 && row.counts(x) > cmax) {
                cmax = row.counts(x)
              }
              if (row.counts(x) > 0 && row.counts(x) < cmin) {
                cmin = row.counts(x)
              }
            } else {
              System.out.println("WTF??????????? Trying to inc in bucket OVER what we want!")
            }
          }
          //        }
        } else {
          System.out.println(
            s"************** WTF? X ${x} vs ${hCells}, Y ${bucketIndex} vs ${buckets.length} @ ${t}"
          )
        }
      }
      t += graphDef.step
    }
  }

  private def enforceCellBounds: Unit = {
    lowerCellBound = plot.heatmapDef.getOrElse(HeatmapDef()).lower.lower(false, cmin)
    upperCellBound = plot.heatmapDef.getOrElse(HeatmapDef()).upper.upper(false, cmax)
    if (lowerCellBound > cmin || upperCellBound < cmax) {
      buckets.foreach { row =>
        for (i <- 0 until row.counts.length) {
          val count = row.counts(i)
          if (count < lowerCellBound || count > upperCellBound) {
            row.counts(i) = 0
          }
        }
      }
    }
  }

  def draw(g: Graphics2D): Unit = {
    enforceCellBounds
    System.out.println("***** Draw heatmap")
    val palette = firstLine.palette.getOrElse(
      Palette.singleColor(firstLine.color)
    )
//    legendMinMax = new Array[(Long, Long, Long)](palette.uniqueColors.size)
//    for (i <- 0 until legendMinMax.length) legendMinMax(i) = (Long.MaxValue, Long.MinValue, 0)
//    val colorScaler =
//      Scales.factory(Scale.LOGARITHMIC)(cmin, cmax, 0, palette.uniqueColors.size - 1)
    // val yi = yticks.iterator
    // var lastY = chartEnd
    buckets.foreach { bucket =>
      // val ytick = if (yi.hasNext) yi.next() else null
      // val nextY = if (ytick != null) yscale(ytick.v) else y1
      if (bucket != null && bucket.counts != null) {
        val lineElement = HeatmapLine(bucket.counts, timeAxis, this)

        // val yy = (chartEnd - bucket.offset + y1 + bucket.height) - bucket.height
        val yy = bucket.y - bucket.height + 1
        // System.out.println(s"YY ${yy} & H ${bucket.height}")
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
      // lastY = nextY
    }
  }

  override def `type`: String = "percentile-heatmap"
}

/**
  *
  * @param boundary
  * @param y
  *   Tick line, top of the bucket. To plot the cell, add the height offset then
  *   it will plot UP from that line.
  * @param height
  * @param prevBoundary
  * @param bktIndex
  * @param split
  */
case class PtileScale(
  base: Double,
  y: Int,
  height: Int,
  next: Double,
  bktIndex: Int,
  skipTick: Boolean,
  majorTick: Boolean,
  // base, isMajor, next
  subTicks: List[(Double, Boolean, Double)] = List.empty
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

  def bktNanos(index: Int): Long = {
    PercentileBuckets.get(index)
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

  def bktIdx(v: Long): Int = {
    PercentileBuckets.indexOf(v)
  }

  /**
    * Percentiles are bucketed inclusive/exclusive. I.e. the call from
    * PercentileBuckets.get(bktIdx) is the exclusive boundary of the NEXT bucket.
    *
    * So .got is the BOTTOM of the bucket and lines on the tick. Plot to the next
    * tick but NOT inclusive, so we should have a dotted line at the top of the graph
    * 
    * @param d1
    * @param d2
    * @param y1
    * @param y2
    * @return
    */
  def getScale(d1: Double, d2: Double, y1: Int, y2: Int): List[PtileScale] = {
    // aiming for about 10px per tick
    val majorTicks = (y2 - y1) / minTickLabelHeight
    val (minBkt, maxBkt) = minMaxBuckets(d1, d2)
    val bktRange = maxBkt - minBkt
    val fillsPerBkt = Math.round((majorTicks * 4) / bktRange.toDouble).toInt
    val avgBktHeight = (y2 - y1).toDouble / bktRange

    var ticks = List.empty[PtileScale]
    var cnt = 0
    var prev = y1.toDouble
    // var prevI = if (minBkt == 0) 0.0 else bktSeconds(minBkt - 1)
    val skipBuckets = bktRange / majorTicks / 4

    for (i <- minBkt until maxBkt) {
      val nextY = prev + avgBktHeight
      val h = (Math.round(nextY) - Math.round(prev)).toInt
      val y = Math.round(prev).toInt
      val base = bktSeconds(if (i > 0) i - 1 else i)
      val nextBucket = bktSeconds(i)

      val subTicks = if (bktRange < majorTicks) {
        val list = List.newBuilder[(Double, Boolean, Double)]
        val delta = (nextBucket - base) / fillsPerBkt
        for (i <- 1 until fillsPerBkt) {
          val v = base + (delta * i)
          val nxt = base + (delta * i + 1)
          list.addOne((v, v == base, nxt))
        }
        list.result()
      } else List.empty

      val skip = !(skipBuckets == 0 || i % skipBuckets == 0) && !(i == minBkt)
      val isMajor = if (skipBuckets == 0 || i % skipBuckets == 0) {
        if (skipBuckets == 0) {
          true
        } else {
          i % majorTicks == 0
        }
      } else false
      ticks = ticks :+ PtileScale(base, y, h, nextBucket, i, skip, isMajor, subTicks)
      cnt += 1
      prev = nextY
    }
    ticks
  }

  def minMaxBuckets(min: Double, max: Double): (Int, Int) = {
    var minBkt = PercentileBuckets.indexOf((min * 1000 * 1000 * 1000).toLong)
    // we need to see if we received a match on an exact bucket boundary as we need
    // to back off one bucket. If not, we may have an explicit bounds or another
    // time series fudging the scale so account for that.
    if (minBkt > 0) {
      val seconds = bktSeconds(minBkt - 1)
      if (Math.abs(seconds - min) < 1e-9) {
        minBkt -= 1
      }
    }
    (
      minBkt,
      PercentileBuckets.indexOf((max * 1000 * 1000 * 1000).toLong)
    )
  }
}
