package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.graphics.HeatMap.choosePalette
import com.netflix.atlas.chart.graphics.HeatMap.defaultDef
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.PercentileHeatMap.getPtileScale
import com.netflix.atlas.chart.graphics.ValueAxis.minTickLabelHeight
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.HeatMapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.spectator.api.histogram.PercentileBuckets

import java.awt.Color
import java.awt.Graphics2D
import java.util.regex.Pattern

case class PercentileHeatMap(
  graphDef: GraphDef,
  plot: PlotDef,
  axis: ValueAxis,
  timeAxis: TimeAxis,
  x1: Int,
  y1: Int,
  x2: Int,
  chartEnd: Int,
  minP: Int,
  maxP: Int,
  leftOffset: Int = 0,
  rightOffset: Int = 0
) extends HeatMap {

  val yticks = axis.ticks(y1, chartEnd)
  val `type`: String = "percentile-heatmap"

  private val ptileScale = getPtileScale(axis.min, axis.max, y1, chartEnd, minP, maxP)
  private val xTicks = timeAxis.ticks(x1 + leftOffset, x2 - rightOffset)
  private val hCells = xTicks.size

  private var cmin = Double.MaxValue
  private var cmax = Double.MinValue
  private var lowerCellBound: Double = 0
  private var upperCellBound: Double = 0
  private var firstLine: LineDef = null
  private var label: String = null

  case class Bkt(
    counts: Array[Double],
    y: Int,
    height: Int,
    v: Double,
    next: Double,
    ticksPerBucket: Int
  )

  val buckets = {
    // NOTE: It is expected that the scale will ALWAYS be PERCENTILE if we get here.
    // If we desire supporting other scales, we need to handle alignment and the
    // single bucket (same min/max on the axis. 0 yticks in that case) use case.

    ptileScale.map(s =>
      Bkt(
        new Array[Double](hCells),
        s.y,
        s.height,
        s.baseDuration,
        s.nextDuration,
        Math.max(1, s.subTicks.size)
      )
    )
//    var lastY = y1
//    val bkts = new Array[Bkt](yticks.size - 1)
//    // if we only have a small number of buckets to plot, we need to distribute the
//    // counts across ticks.
//    val ticksPerBucket =
//      if (yticks.size - 1 >= ptileScale.size) {
//        Math.max(1, (yticks.size - 1) / ptileScale.size)
//      } else 1
//    for (i <- yticks.length - 2 to 0 by -1) {
//      val tick = yticks(i)
//      val y = yscale(tick.v)
//      val nextTick = yticks(i + 1)
//      bkts(i) = Bkt(new Array[Double](hCells), y, y - lastY, tick.v, nextTick.v, ticksPerBucket)
//      lastY = y
//    }
    // bkts
  }

  {
    plot.lines.zip(plot.data).filter(_._1.lineStyle == LineStyle.HEATMAP).foreach { tuple =>
      val (line, data) = tuple
      if (firstLine == null) {
        firstLine = line
        // TODO - if the user explicitly provides a label via heatmap options or :legend,
        // naturally we'll use that. But it will only grab the FIRST of the expressions in
        // the heatmap. If there is no label, we can try falling back to the first query.
        // Failing that, the ylabel then just the string "Heatmap".
        label = plot.heatmapDef.getOrElse(defaultDef).legend.getOrElse {
          if (data.label != null && data.label.nonEmpty) {
            data.label
          } else {
            line.query.getOrElse(plot.ylabel.getOrElse("Heatmap"))
          }
        }
      }
      addLine(line)
    }
    enforceCellBounds
  }

  def draw(g: Graphics2D): Unit = {
    buckets.foreach { bucket =>
      val lineElement = HeatMapRow(bucket.counts, timeAxis, this)
      val yy = bucket.y
      lineElement.draw(
        g,
        x1 + leftOffset,
        yy,
        x2 - rightOffset,
        Math.max(1, bucket.height)
      )
    }

//    Style(Color.RED).configure(g)
//    g.drawLine(x1 + leftOffset, 274, x2 - rightOffset, 274)
  }

  override def legendLabel: String = label

  def rows: Array[Array[Double]] = {
    val results = new Array[Array[Double]](buckets.length)
    buckets.zipWithIndex.foreach { tuple =>
      val (bkt, idx) = tuple
      results(idx) = bkt.counts
    }
    results
  }

  protected[graphics] lazy val palette = choosePalette(firstLine)

  protected[graphics] lazy val colorScaler =
    HeatMap.colorScaler(plot, palette, lowerCellBound, upperCellBound)

  private def addLine(line: LineDef): Unit = {
    val seconds = bktSeconds(line)

//    var tn = graphDef.startTime.toEpochMilli
//    var cnt = 0
//    var sum = 0.0
//    var min = Double.MaxValue
//    var max = Double.MinValue
//    while (tn < graphDef.endTime.toEpochMilli) {
//      val v = line.data.data(tn)
//      System.out.print(s"${v}, ")
//      if (v.isFinite && v > 0) {
//        cnt += 1
//        sum += v
//        if (v > max) max = v
//        if (v < min) min = v
//      }
//      tn += line.data.data.step
//    }
//    System.out.println()
//    System.out.println(s"Count for ${seconds} is ${cnt} and sum ${sum} min ${min} max ${max}")
    // axis bounds check
    if (seconds > yticks.last.v || seconds < yticks.head.v) {
      return
    }

    // we need to figure out how many buckets to fill. This is due to the regular
    // ticks scale but exponential bucket size. Higher buckets can spread across
    // cells.
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
      throw new IllegalStateException(s"No bucket?????? for ${seconds}")
    }
    val row = buckets(bucketIndex)
    System.out.println(s"Row ${row.v} @ ${row.y}")
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
          val count = v / tpb
          // for (i <- 0 until tpb) {
          // if (bucketIndex + i < buckets.length) { // edge case if there is only a single bucket.
          // row = buckets(bucketIndex + i)
          if (seconds >= 0) row.counts(x) += count else row.counts(x) += 1

          if (row.counts(x) > 0 && row.counts(x) > cmax) {
            cmax = row.counts(x)
          }
          if (row.counts(x) > 0 && row.counts(x) < cmin) {
            cmin = row.counts(x)
          }
          // }
          // }
        }
      }
      t += graphDef.step
    }
  }

  private def enforceCellBounds: Unit = {
    lowerCellBound = plot.heatmapDef.getOrElse(HeatMapDef()).lower.lower(false, cmin)
    upperCellBound = plot.heatmapDef.getOrElse(HeatMapDef()).upper.upper(false, cmax)
    buckets.foreach { row =>
      for (i <- 0 until row.counts.length) {
        val count = row.counts(i)
        if (count < lowerCellBound || count > upperCellBound) {
          row.counts(i) = 0
        } else {
          updateLegend(count, colorScaler(count))
        }
      }
    }
  }
}

/**
  * An entry aligned to a Spectator percentile bucket.
  *
  * @param baseDuration
  *   The inclusive base duration in seconds.
  * @param y
  *   Tick line, top of the bucket. To plot the cell, add the height offset then
  *   it will plot UP from that line.
  * @param height
  *   The height of the bucket for plotting.
  * @param nextDuration
  *   The next, exclusive bucket duration in seconds.
  * @param skipTick
  *   Whether or not to skip plotting a tick for this bucket.
  * @param majorTick
  *   Whether or not the tick is a major tick.
  * @param subTicks
  *   An optional list of sub ticks if there are few buckets for the plot.
  *   Tuple is (baseDuration, isMajor, nextDuration)
  */
case class PtileScale(
  baseDuration: Double,
  y: Int,
  height: Int,
  nextDuration: Double,
  skipTick: Boolean,
  majorTick: Boolean,
  bkts: Int,
  subTicks: List[(Double, Boolean, Double)] = List.empty
)

object PercentileHeatMap {

  private val timerBucketIdPattern = Pattern.compile("^T[0-9A-F]{4}$")

  private val percentileBucketsCount = PercentileBuckets.asArray().length

  /**
    * Determines if the line is a Spectator percentile timer by looking to see if
    * the line is grouped on the `percentile` tag and has a `T####` value.
    *
    * @param line
    *   A non-null line to parse.
    * @return
    *   True if the line is a spectator percentile, false if not.
    */
  def isSpectatorPercentile(line: LineDef): Boolean = {
    line.data.tags.get("percentile") match {
      case Some(v) => timerBucketIdPattern.matcher(v).find()
      case _       => false
    }
  }

  /**
    * Determines if the line is a Spectator percentile timer by looking to see if
    * the line is grouped on the `percentile` tag and has a `T####` value.
    *
    * @param tags
    *   A non-null map of tags for the time series.
    * @return
    *   True if the line is a spectator percentile, false if not.
    */
  def isSpectatorPercentile(tags: Map[String, String]): Boolean = {
    tags.get("percentile") match {
      case Some(v) => timerBucketIdPattern.matcher(v).find()
      case _       => false
    }
  }

  /**
    * Returns the inclusive nanoseconds duration for the given percentile bucket.
    *
    * @param line
    *   A non-null line where the `percentile` tag value is parsed.
    * @return
    *   A nanoseconds value matching the Spectator percentile bucket lower boundary.
    */
  def bktNanos(line: LineDef): Long = {
    PercentileBuckets.get(bktIdx(line))
  }

  /**
    * Returns the inclusive nanoseconds duration for the given percentile bucket.
    *
    * @param index
    *   An index into the {@link PercentileBuckets.asArray()} array.
    * @return
    *   A nanoseconds value matching the Spectator percentile bucket lower boundary.
    */
  def bktNanos(index: Int): Long = {
    PercentileBuckets.get(if (index > 0) index - 1 else index)
  }

  /**
    * Returns the inclusive seconds duration for the given percentile bucket.
    *
    * @param line
    *   A non-null line where the `percentile` tag value is parsed.
    * @return
    *   A seconds value matching the Spectator percentile bucket lower boundary.
    */
  def bktSeconds(line: LineDef): Double = {
    val idx = bktIdx(line)
    PercentileBuckets.get(if (idx > 0) idx - 1 else idx) / 1000.0 / 1000.0 / 1000.0
  }

  /**
    * Returns the inclusive seconds duration for the given percentile bucket.
    *
    * @param bkt
    *   An index into the {@link PercentileBuckets.asArray()} array.
    * @return
    *   A seconds value matching the Spectator percentile bucket lower boundary.
    */
  def bktSeconds(bkt: Int): Double = {
    PercentileBuckets.get(if (bkt > 0) bkt - 1 else bkt) / 1000.0 / 1000.0 / 1000.0
  }

  /**
    * Returns the index for the given percentile bucket in
    * {@link PercentileBuckets.asArray()}
    *
    * @param line
    *   A non-null line def to pull the `percentile` tag value from.
    * @return
    *   The index of the bucket.
    */
  def bktIdx(line: LineDef): Int = {
    Integer.parseInt(line.data.tags("percentile").substring(1), 16)
  }

  /**
    * Returns the index of a bucket the value should fall into.
    * **WARNING** Note that calling this method with the duration returned from a
    * {@link bktNanos} call will return the **NEXT** bucket index instead of the
    * expected index.
    *
    * @param v
    *   The measurement in nanoseconds.
    * @return
    *   The index of a bucket from {@link PercentileBuckets.asArray()}.
    */
  def bktIdx(v: Long): Int = {
    PercentileBuckets.indexOf(v)
  }

  /**
    * Computes a list of Spectator buckets in range of the values along with their y
    * scales and whether or not the ticks should be major or omitted. If a plot has
    * a small number of buckets relative to the y axis, buckets are split into sub-
    * ticks to distribute the counts across minor ticks.
    *
    * @param d1
    *   The min value to be plotted in seconds. Note that it MAY be a bucket boundary
    *   for the NEXT bucket.
    * @param d2
    *   The max value to be plotted in seconds. Note that it MAY be a bucket boundary
    *   for the NEXT bucket.
    * @param y1
    *   The start of the y axis from the top of the graph.
    * @param y2
    *   The end of the chart area from the top of the graph.
    * @return
    *   A non-empty list of percentile bucket descriptors.
    */
  def getPtileScale(
    d1: Double,
    d2: Double,
    y1: Int,
    y2: Int,
    minP: Int,
    maxP: Int
  ): List[PtileScale] = {
    // aiming for about 10px per tick
    val pixelSpan = y2 - y1
    var pixelsPerPercentile = 10.0
    val initBkts = pixelSpan / pixelsPerPercentile

    val minBkt = {
      if (d1 < bktSeconds(minP)) if (minP > 0) minP - 1 else minP
      else minP
    }
    var maxBkt = {
      val max = if (maxP + 1 < percentileBucketsCount) maxP + 1 else maxP
      val s = bktSeconds(max)
      if (d2 > s) if (max + 1 < percentileBucketsCount) max + 1 else max
      else max
    }
    // var (minBkt, maxBkt) = minMaxBuckets(d1, d2)
    if (minBkt == maxBkt && maxBkt + 1 < percentileBucketsCount) {
      maxBkt += 1
    }
    val bktRange = Math.max(1, maxBkt - minBkt)

    val bkts = List.newBuilder[PtileScale]
    val bktsPerTick = {
      if (bktRange / initBkts < 1) {
        0
      } else {
        Math.ceil(bktRange / initBkts).toInt
      }
    }

    if (bktsPerTick < 1) {
      // spread the buckets out
      val subsPerTick = (initBkts / bktRange).toInt
      pixelsPerPercentile = pixelSpan / bktRange.doubleValue()
      var prev = y2.toDouble + 1
      for (i <- minBkt until maxBkt) {
        val base = bktSeconds(i)
        val next = bktSeconds(i + 1)

        val list = List.newBuilder[(Double, Boolean, Double)]
        val delta = (next - base) / subsPerTick
        for (x <- 1 until subsPerTick) {
          val v = base + (delta * x)
          val nxt = base + (delta * x + 1)
          list.addOne((v, v == base, nxt))
        }

        val y = prev - pixelsPerPercentile
        val h = (Math.round(prev) - Math.round(y)).toInt

        bkts += PtileScale(base, Math.round(y).toInt, h, next, false, true, 1, list.result())

        prev = y
      }
    } else {
      pixelsPerPercentile = pixelSpan / (bktRange.toDouble)

      val initBktsPerTick = {
        val floorBuckets = Math.floor(bktRange / bktsPerTick).toInt
        bktRange - (floorBuckets * bktsPerTick)
      }

      var bkt = minBkt
      var prev = y2.toDouble + 1
      while (bkt < maxBkt) {
        val bpt = if (bkt == minBkt && initBktsPerTick > 0) initBktsPerTick else bktsPerTick
        val base = bktSeconds(bkt)
        var nxtBkt = bkt + bpt
        if (nxtBkt > maxBkt)
          nxtBkt = maxBkt
        val ptilesInScale = nxtBkt - bkt

        val next = bktSeconds(nxtBkt)
        val y = prev - (pixelsPerPercentile * ptilesInScale)
        val h = (Math.round(prev) - Math.round(y)).toInt

        bkts += PtileScale(
          base,
          Math.round(y).toInt,
          h,
          next,
          false,
          bkt == minBkt || bkt - initBktsPerTick != minBkt,
          ptilesInScale,
          List.empty
        )
        bkt = nxtBkt
        prev = y
        // ctr += 1
      }
      // ctr += 1
    }

    bkts += PtileScale(
      bktSeconds(maxBkt),
      y1,
      0,
      bktSeconds(maxBkt + 1),
      false,
      true, // ctr == nxtMgr,
      1,
      List.empty
    )

    bkts.result()
  }

  def getPtileScale__OLD(d1: Double, d2: Double, y1: Int, y2: Int): List[PtileScale] = {
    // aiming for about 10px per tick
    val majorTicks = (y2 - y1) / minTickLabelHeight
    val (minBkt, maxBkt) = minMaxBuckets(d1, d2)
    val bktRange = Math.max(1, maxBkt - minBkt)
    val fillsPerBkt = Math.round((majorTicks * 4) / bktRange.toDouble).toInt
    val avgBktHeight = (y2 - y1).toDouble / (bktRange + 1)

    var ticks = List.empty[PtileScale]
    var cnt = 0
    var prev = y1.toDouble
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

      ticks = ticks :+ PtileScale(base, y, h, nextBucket, skip, isMajor, 0, subTicks)
      cnt += 1
      prev = nextY
    }
    ticks
  }

  /**
    * Computes the Spectator bucket indexes for the given min/max values.
    * Note that since buckets are [lower, upper), the indices returned from {@link indexOf}
    * for the duration returned from {@link bktSeconds) are for the **NEXT** bucket
    * instead of the bucket where the value should reside. Thus this method attempts
    * to detect if the min/max are coming from {@link bktSeconds} or actual values
    * and adjust min down or max up for a proper range.
    *
    * @param min
    *   The min value to be plotted in seconds. Note that it MAY be a bucket boundary
    *   for the NEXT bucket.
    * @param max
    *   The max value to be plotted in seconds. Note that it MAY be a bucket boundary
    *   for the NEXT bucket.
    * @return
    *   A tuple with the min and max bucket indices.
    */
  def minMaxBuckets(min: Double, max: Double): (Int, Int) = {
    val minBkt = bktIdx((min * 1000 * 1000 * 1000).toLong)
    // we need to see if we received a match on an exact bucket boundary as we need
    // to back off one bucket. If not, we may have an explicit bounds or another
    // time series fudging the scale so account for that.
//    if (minBkt > 0) {
//      val seconds = bktSeconds(minBkt - 1)
//      if (Math.abs(seconds - min) < 1e-9) {
//        minBkt -= 1
//      }
//    }

    var maxBkt = bktIdx((max * 1000 * 1000 * 1000).toLong)
//    if (maxBkt < percentileBucketsCount) {
//      maxBkt += 1
//    }
//    if (maxBkt < percentileBucketsCount && maxBkt > 0) {
//      val seconds = bktSeconds(maxBkt - 1)
//      if (max > seconds) {
//        maxBkt += 1
//      }
//    }

    // edge case for 0
    if (minBkt == 0 && maxBkt == 0) maxBkt = 1
    (minBkt, maxBkt)
  }
}
