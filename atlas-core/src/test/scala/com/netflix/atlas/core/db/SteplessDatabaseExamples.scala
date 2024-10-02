package com.netflix.atlas.core.db

import com.netflix.atlas.core.index.TagIndex
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.MapMeta
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.Stepless.assertEqualsWithMeta
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import scala.collection.mutable.TreeMap
import scala.collection.mutable

class SteplessDatabaseExamples extends FunSuite {

  val end = 1706572800000L
  val limit = 5
  val dtFormat = DateTimeFormatter.ofPattern("dd'T'HH:mm").withZone(ZoneId.of("UTC"))

  val db = new Database {

    val hourlyTags1 = Map("name" -> "hourly", "node" -> "i-1")
    val hourlyTags2 = Map("name" -> "hourly", "node" -> "i-2")

    val hourlySeries1 = {
      val tree = mutable.TreeMap.newBuilder[Long, Double](Ordering.Long.reverse)
      for (i <- limit * 5 until 0 by -1) {
        val factor = (limit * 5) - i
        tree += (end - (3600_000L * factor)) -> i
      }
      tree.result()
    }

    // doesn't report until the 13th hour
    val hourlySeries2 = {
      val tree = mutable.TreeMap.newBuilder[Long, Double](Ordering.Long.reverse)
      for (i <- limit * 5 until 0 by -1) {
        if (i > 13) {
          val factor = (limit * 5) - i
          tree += (end - (3600_000L * factor)) -> i
        }
      }
      tree.result()
    }

    override def index: TagIndex[_ <: TaggedItem] = ???

    override def execute(ctxt: EvalContext, expr: DataExpr): List[TimeSeries] = {
      if (ctxt.steplessLimit.isEmpty) {
        throw new IllegalArgumentException("steplessLimit is required")
      }
      val offset = expr.offset.getNano / 1_000_000L

      var max = 0
      val series = List.newBuilder[TimeSeries]
      expr.toString match {
        case s if s.contains(":by") =>
          var tuple = toSeries(ctxt, offset, hourlySeries1, hourlyTags1)
          if (tuple._1 > max) {
            max = tuple._1
          }
          series += offsetMaybe(offset, tuple._2)
          tuple = toSeries(ctxt, offset, hourlySeries2, hourlyTags2, Some(max))
          if (tuple._1 > max) {
            max = tuple._1
          }
          series += offsetMaybe(offset, tuple._2)
        case s if s.contains("hourly") =>
          val tuple = toSeries(ctxt, offset, hourlySeries1, hourlyTags1)
          max = tuple._1
          series += offsetMaybe(offset, tuple._2)
        case _ => // no-data
      }

      if (max > 0) {
        ctxt.update(0, max)
      } else {
        ctxt.update(0, ctxt.steplessLimit.get)
      }
      expr.eval(ctxt, series.result()).data
    }

    def offsetMaybe(offset: Long, series: TimeSeries): TimeSeries = {
      if (offset > 0) {
        series.offset(offset)
      } else {
        series
      }
    }

    def toSeries(
      eval: EvalContext,
      offset: Long,
      data: TreeMap[Long, Double],
      tags: Map[String, String],
      expected: Option[Int] = None
    ): (Int, TimeSeries) = {
      val slice = data.range(eval.queryEnd, eval.queryStart)
      if (slice.isEmpty) {
        return (0, eval.noData)
      }

      val range = offset + eval.steplessLimit.get
      var read = 0
      val it = slice.iterator
      val builder = List.newBuilder[(Long, Double)]
      while (it.hasNext) {
        val (t, v) = it.next()
        if (read >= offset && read < range) {
          builder += (t -> v)
        }
        read += 1
      }

      val results = builder.result()
      if (results.isEmpty) {
        return (0, eval.noData)
      }
      val padding = expected.map(e => e - results.size).getOrElse(0)
      val array = if (padding > 0) {
        val pad = Array.fill(padding + results.size)(Double.NaN)
        var i = padding
        results.reverse.foreach { tuple =>
          pad(i) = tuple._2
          i += 1
        }
        pad
      } else {
        results.map(_._2).toArray.reverse
      }

      val seq = new ArrayTimeSeq(DsType.Gauge, -offset, eval.step, array)
      val metaMaps = results.map { tuple =>
        val ts = tuple._1
        Map("timestamp" -> dtFormat.format(Instant.ofEpochMilli(ts)))
      }
      val meta = Map.newBuilder[Long, Map[String, String]]
      for (i <- 0 until metaMaps.size) {
        val ts = results.size - 1 - i - offset + padding
        meta += (ts -> metaMaps(i))
      }
      val res = meta.result()
      (results.size, TimeSeries(tags, seq, Some(MapMeta(res))))
    }
  }

  test("single series - full limit") {
    val ctxt =
      new EvalContext(endMinusHours(5), end, 1, steplessLimit = Some(limit))
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"))
    val actual = db.execute(ctxt, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(21.0, 22.0, 23.0, 24.0, 25.0)),
        Some(
          MapMeta(
            Map(
              4L -> Map("timestamp" -> "30T00:00"),
              3L -> Map("timestamp" -> "29T23:00"),
              2L -> Map("timestamp" -> "29T22:00"),
              1L -> Map("timestamp" -> "29T21:00"),
              0L -> Map("timestamp" -> "29T20:00")
            )
          )
        )
      )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 5L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - full limit, query window") {
    val ctxt =
      new EvalContext(endMinusHours(10), endMinusHours(5), 1, steplessLimit = Some(limit))
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"))
    val actual = db.execute(ctxt, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(16.0, 17.0, 18.0, 19.0, 20.0)),
        Some(
          MapMeta(
            Map(
              4L -> Map("timestamp" -> "29T19:00"),
              3L -> Map("timestamp" -> "29T18:00"),
              2L -> Map("timestamp" -> "29T17:00"),
              1L -> Map("timestamp" -> "29T16:00"),
              0L -> Map("timestamp" -> "29T15:00")
            )
          )
        )
      )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 5L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - under limit, partial query window") {
    val ctxt =
      new EvalContext(endMinusHours(30), endMinusHours(23), 1, steplessLimit = Some(limit))
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"))
    val actual = db.execute(ctxt, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(1.0, 2.0)),
        Some(
          MapMeta(
            Map(
              1L -> Map("timestamp" -> "29T01:00"),
              0L -> Map("timestamp" -> "29T00:00")
            )
          )
        )
      )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 2L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - no data in window") {
    val ctxt =
      new EvalContext(endMinusHours(30), endMinusHours(25), 1, steplessLimit = Some(limit))
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"))
    val actual = db.execute(ctxt, expr).head
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 5L)
    assertEquals(actual.label, "NO DATA")
  }

  test("single series - with full offset") {
    val ctxt =
      new EvalContext(endMinusHours(10), end, 1, steplessLimit = Some(limit))
    val offsetCtx = ctxt.withOffset(5)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"), offset = Duration.ofMillis(4))
    val actual = db.execute(offsetCtx, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(16.0, 17.0, 18.0, 19.0, 20.0)),
        Some(
          MapMeta(
            Map(
              4L -> Map("timestamp" -> "29T19:00"),
              3L -> Map("timestamp" -> "29T18:00"),
              2L -> Map("timestamp" -> "29T17:00"),
              1L -> Map("timestamp" -> "29T16:00"),
              0L -> Map("timestamp" -> "29T15:00")
            )
          )
        )
      )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 5L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - offset limited due to query window") {
    val ctxt =
      new EvalContext(endMinusHours(5), end, 1, steplessLimit = Some(limit))
    val offsetCtx = ctxt.withOffset(5)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"), offset = Duration.ofMillis(4))
    val actual = db.execute(offsetCtx, expr).head
    ctxt.update(offsetCtx)
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(20.0)),
        Some(
          MapMeta(
            Map(
              0L -> Map("timestamp" -> "29T19:00")
            )
          )
        )
      )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 1L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series with partial offset") {
    val ctxt =
      new EvalContext(endMinusHours(25), end, 1, steplessLimit = Some(limit))
    val offsetCtx = ctxt.withOffset(23)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"), offset = Duration.ofMillis(23))
    val actual = db.execute(ctxt, expr).head
    ctxt.update(offsetCtx)
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(1.0, 2.0)),
        Some(
          MapMeta(
            Map(
              1L -> Map("timestamp" -> "29T01:00"),
              0L -> Map("timestamp" -> "29T00:00")
            )
          )
        )
      )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 2L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("group by - full limit") {
    val ctxt =
      new EvalContext(endMinusHours(5), end, 1, steplessLimit = Some(limit))
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "hourly")), List("name"))
    val actual = db.execute(ctxt, expr).head
    val expected = TimeSeries(
      Map("name" -> "hourly"),
      "(name=hourly)",
      new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(42.0, 44.0, 46.0, 48.0, 50.0)),
      Some(
        MapMeta(
          Map(
            4L -> Map("timestamp" -> "30T00:00"),
            3L -> Map("timestamp" -> "29T23:00"),
            2L -> Map("timestamp" -> "29T22:00"),
            1L -> Map("timestamp" -> "29T21:00"),
            0L -> Map("timestamp" -> "29T20:00")
          )
        )
      )
    )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 5L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("group by - full limit, one series missing at start") {
    val ctxt =
      new EvalContext(endMinusHours(15), endMinusHours(10), 1, steplessLimit = Some(limit))
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "hourly")), List("name"))
    val actual = db.execute(ctxt, expr).head
    val expected = TimeSeries(
      Map("name" -> "hourly"),
      "(name=hourly)",
      new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(11.0, 12.0, 13.0, 28.0, 30.0)),
      Some(
        MapMeta(
          Map(
            // TODOD - do we want this behavior? In this case we _could_ keep it, but not in other cases.
            4L -> Map("timestamp" -> "29T14:00"),
            3L -> Map("timestamp" -> "29T13:00")
          )
        )
      )
    )
    assertEquals(ctxt.start, 0L)
    assertEquals(ctxt.end, 5L)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  def endMinusHours(hours: Long): Long = {
    end - (3600_000L * hours)
  }
}
