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

  val dtFormat = DateTimeFormatter.ofPattern("dd'T'HH:mm").withZone(ZoneId.of("UTC"))
  val end = 1706572800000L

  val db = new Database {

    val hourlyTags1 = Map("name" -> "hourly", "node" -> "i-1")
    val hourlyTags2 = Map("name" -> "hourly", "node" -> "i-2")

    val hourlySeries1 = {
      val tree = mutable.TreeMap.newBuilder[Long, Double](Ordering.Long.reverse)
      for (i <- 5 * 5 until 0 by -1) {
        val factor = (5 * 5) - i
        tree += (end - (3600_000L * factor)) -> i
      }
      tree.result()
    }

    // doesn't report until the 13th hour
    val hourlySeries2 = {
      val tree = mutable.TreeMap.newBuilder[Long, Double](Ordering.Long.reverse)
      for (i <- 5 * 5 until 0 by -1) {
        if (i > 13) {
          val factor = (5 * 5) - i
          tree += (end - (3600_000L * factor)) -> i
        }
      }
      tree.result()
    }

    override def index: TagIndex[_ <: TaggedItem] = ???

    override def execute(ctxt: EvalContext, expr: DataExpr): List[TimeSeries] = {
      if (!ctxt.runMode) {
        throw new IllegalArgumentException("Must run in runmode")
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
          tuple = toSeries(ctxt, offset, hourlySeries2, hourlyTags2)
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
      tags: Map[String, String]
    ): (Int, TimeSeries) = {
      if (data.isEmpty) {
        return (0, eval.noData)
      }

      val start = eval.start + (offset * 2)
      val end = eval.end + (offset * 2)
      var read = 0
      val it = data.iterator
      val builder = List.newBuilder[(Long, Double)]
      val meta = Map.newBuilder[Long, Map[String, String]]
      var invert = end - 1L
      while (it.hasNext) {
        val (t, v) = it.next()
        if (read < end && read >= start) {
          builder += (t   -> v)
          meta += (invert -> Map("timestamp" -> dtFormat.format(Instant.ofEpochMilli(t))))
          invert -= 1
        }
        read += 1
      }

      val results = builder.result()
      if (results.isEmpty) {
        return (0, eval.noData)
      }
      val padding = (end - start).toInt - results.size
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

      val seq = new ArrayTimeSeq(DsType.Gauge, start, eval.step, array)
      val res = meta.result()
      (results.size, TimeSeries(tags, seq, Some(MapMeta(res))))
    }
  }

  test("single series - 0 to 5") {
    val ctxt = EvalContext(0, 5, 1, runMode = true)
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

  test("single series - 5 to 10") {
    val ctxt = EvalContext(5, 10, 1, runMode = true)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"))
    val actual = db.execute(ctxt, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 5, 1, Array(16.0, 17.0, 18.0, 19.0, 20.0)),
        Some(
          MapMeta(
            Map(
              9L -> Map("timestamp" -> "29T19:00"),
              8L -> Map("timestamp" -> "29T18:00"),
              7L -> Map("timestamp" -> "29T17:00"),
              6L -> Map("timestamp" -> "29T16:00"),
              5L -> Map("timestamp" -> "29T15:00")
            )
          )
        )
      )
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - 23 to 28, partial range") {
    val ctxt = EvalContext(23, 28, 1, runMode = true)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"))
    val actual = db.execute(ctxt, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 23, 1, Array(Double.NaN, Double.NaN, Double.NaN, 1.0, 2.0)),
        Some(
          MapMeta(
            Map(
              27L -> Map("timestamp" -> "29T01:00"),
              26L -> Map("timestamp" -> "29T00:00")
            )
          )
        )
      )
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - 25 to 30, out of range") {
    val ctxt = EvalContext(25, 30, 1, runMode = true)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"))
    val actual = db.execute(ctxt, expr).head
    assertEquals(actual.label, "NO DATA")
  }

  test("single series - 0 to 5 with offset in range") {
    val ctxt = EvalContext(0, 5, 1, runMode = true)
    val offsetCtx = ctxt.withOffset(5)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"), offset = Duration.ofMillis(5))
    val actual = db.execute(offsetCtx, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(16.0, 17.0, 18.0, 19.0, 20.0)),
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
      ).offset(5)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - 0 to 5 with offset in partial range") {
    val ctxt = EvalContext(0, 5, 1, runMode = true)
    val offsetCtx = ctxt.withOffset(23)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"), offset = Duration.ofMillis(23))
    val actual = db.execute(offsetCtx, expr).head
    val expected =
      TimeSeries(
        Map("name" -> "hourly"),
        new ArrayTimeSeq(DsType.Gauge, 23, 1, Array(Double.NaN, Double.NaN, Double.NaN, 1.0, 2.0)),
        Some(
          MapMeta(
            Map(
              27L -> Map("timestamp" -> "29T01:00"),
              26L -> Map("timestamp" -> "29T00:00")
            )
          )
        )
      ).offset(23)
    assertEqualsWithMeta(ctxt, actual, expected)
  }

  test("single series - 0 to 5 with offset in out of range") {
    val ctxt = EvalContext(0, 5, 1, runMode = true)
    val offsetCtx = ctxt.withOffset(25)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"), offset = Duration.ofMillis(25))
    val actual = db.execute(offsetCtx, expr).head
    assertEquals(actual.label, "NO DATA (offset=PT0.025S)")
  }

  test("single series - 20 to 25 with offset in out of range") {
    val ctxt = EvalContext(20, 25, 1, runMode = true)
    val offsetCtx = ctxt.withOffset(25)
    val expr = DataExpr.Sum(Query.Equal("name", "hourly"), offset = Duration.ofMillis(25))
    val actual = db.execute(offsetCtx, expr).head
    assertEquals(actual.label, "NO DATA (offset=PT0.025S)")
  }

  test("group by - 0 to 5") {
    val ctxt = EvalContext(0, 5, 1, runMode = true)
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

  test("group by - 5 to 10, one series missing at start") {
    val ctxt = EvalContext(10, 15, 1, runMode = true)
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "hourly")), List("name"))
    val actual = db.execute(ctxt, expr).head
    val expected = TimeSeries(
      Map("name" -> "hourly"),
      "(name=hourly)",
      new ArrayTimeSeq(DsType.Gauge, 10, 1, Array(11.0, 12.0, 13.0, 28.0, 30.0)),
      Some(
        MapMeta(
          Map(
            // TODOD - do we want this behavior? In this case we _could_ keep it, but not in other cases.
            14L -> Map("timestamp" -> "29T14:00"),
            13L -> Map("timestamp" -> "29T13:00")
          )
        )
      )
    )
    assertEqualsWithMeta(ctxt, actual, expected)
  }
}
