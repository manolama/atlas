package com.netflix.atlas.core.model

import com.netflix.atlas.core.model.TimeSeriesSuite.ts
import munit.FunSuite
import org.junit.Assert.assertFalse

object TimeSeriesSuite {

  def ts(context: EvalContext, values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, context.start, context.step, values.toArray)
    TimeSeries(Map("name" -> "cpu", "node" -> "i-1"), seq, None)
  }

  def ts(start: Long, step: Long, values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, step, values.toArray)
    TimeSeries(Map("name" -> "cpu", "node" -> "i-1"), seq, None)
  }

}

class TimeSeriesSuite extends FunSuite {

  test("timeseries w meta: unaryOp") {
    val context = Stepless.steplessContext(5)
    val input = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val expected = Stepless.ts(context, 2.0, 4.0, 6.0, 8.0, 10.0)
    val actual = input.unaryOp("UT: %s", v => v * 2.0)
    Stepless.assertEqualsWithMetaFunc(context, actual, expected)
    assertEquals(actual.label, "UT: name=cpu, node=i-1")
  }

  test("timeseries w meta: binaryOp - both have meta") {
    val context = Stepless.steplessContext(5)
    val input1 = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val input2 = Stepless.ts(context, 1.5, 2.5, 3.5, 4.5, 5.5)
    val expected = Stepless.ts(context, 2.5, 4.5, 6.5, 8.5, 10.5)
    val actual = input1.binaryOp(input2, "UT: %s", (a, b) => a + b)
    Stepless.assertEqualsWithMetaFunc(context, actual, expected)
    assertEquals(actual.label, "UT: name=cpu, node=i-1")
  }

  test("timeseries w meta: binaryOp - one with offset") {
    val context = Stepless.steplessContext(5)
    val seq = new ArrayTimeSeq(DsType.Gauge, -5, 1, Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val input1 = MetaWrapper(
      TimeSeries(Map("name" -> "cpu", "node" -> "i-1"), seq, None),
      List("job", s"Different {i}")
    ).offset(5)
    val input2 = Stepless.ts(context, 1.5, 2.5, 3.5, 4.5, 5.5)
    val expected = Stepless.ts(context, 2.5, 4.5, 6.5, 8.5, 10.5)
    val actual = input1.binaryOp(input2, "UT: %s", (a, b) => a + b)
    for (i <- context.start until context.end) {
      assertEqualsDouble(
        actual.data(i),
        expected.data(i),
        0.00001,
        s"values are not the same at index ${i}"
      )
      // with offsets, the meta will be different so we'll likely wind up losing it.
      assertFalse(actual.meta.get.datapointMeta(i).isDefined)
    }
    assertEquals(actual.label, "UT: name=cpu, node=i-1")
  }

  test("timeseries w meta: binaryOp - one has meta") {
    val context = Stepless.steplessContext(5)
    val input1 = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val input2 = ts(context, 1.5, 2.5, 3.5, 4.5, 5.5)
    val expected = Stepless.ts(context, 2.5, 4.5, 6.5, 8.5, 10.5)
    val actual = input1.binaryOp(input2, "UT: %s", (a, b) => a + b)
    Stepless.assertEqualsWithOutMeta(context, actual, expected)
    assertEquals(actual.label, "UT: name=cpu, node=i-1")
  }

  test("timeseries w meta: withTags") {
    val context = Stepless.steplessContext(5)
    val input = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val expected = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val actual = input.withTags(Map("foo" -> "bar"))
    Stepless.assertEqualsWithMetaFunc(context, actual, expected)
    assertEquals(actual.label, "name=cpu, node=i-1")
    assertEquals(input.tags, Map("name" -> "cpu", "node" -> "i-1"))
    assertEquals(actual.tags, Map("foo" -> "bar"))
  }

  test("timeseries w meta: withLabel") {
    val context = Stepless.steplessContext(5)
    val input = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val expected = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val actual = input.withLabel("UT")
    Stepless.assertEqualsWithMetaFunc(context, actual, expected)
    assertEquals(actual.label, "UT")
  }

  test("timeseries w meta: consolidate") {
    val context = Stepless.steplessContext(5)
    val input = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val cctxt = EvalContext(context.start, context.end, 2, runMode = true)
    val expected = Stepless.ts(cctxt, 3.0, 7.0, 5.0)
    val actual = input.consolidate(2, ConsolidationFunction.Sum)
    Stepless.assertEqualsWithMetaFunc(cctxt, actual, expected)
  }

  test("timeseries w meta: blend - both have meta") {
    val context = Stepless.steplessContext(5)
    val input1 = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val input2 = Stepless.ts(context, 1.5, 2.5, Double.NaN, 1.5, 2.5)
    val expected = Stepless.ts(context, 1.5, 2.5, 3.0, 4.0, 5.0)
    val actual = input1.blend(input2)
    Stepless.assertEqualsWithMetaFunc(context, actual, expected)
  }

  test("timeseries w meta: blend - one has meta") {
    val context = Stepless.steplessContext(5)
    val input1 = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val input2 = ts(context, 1.5, 2.5, Double.NaN, 1.5, 2.5)
    val expected = Stepless.ts(context, 1.5, 2.5, 3.0, 4.0, 5.0)
    val actual = input1.blend(input2)
    Stepless.assertEqualsWithOutMeta(context, actual, expected)
  }

  test("timeseries w meta: mapTimeSeq") {
    val context = Stepless.steplessContext(5)
    val input = Stepless.ts(context, 1.0, 2.0, 3.0, 4.0, 5.0)
    val expected = Stepless.ts(context, 0.0, 1.0, 2.0, 3.0, 4.0)
    val actual = input.mapTimeSeq(seq => seq.mapValues(v => v - 1.0))
    Stepless.assertEqualsWithMetaFunc(context, actual, expected)
  }

  test("timeseries w meta: offset") {
    val context = EvalContext(0, 5, 1, runMode = true)
    val input = Stepless.ts(-5, 1.0, 2.0, 3.0, 4.0, 5.0)
    val expected = TimeSeries(
      Map("name" -> "hourly"),
      new ArrayTimeSeq(DsType.Gauge, -5, 1, Array(1.0, 2.0, 3.0, 4.0, 5.0)),
      Some(
        MapMeta(
          Map(
            -5L -> Map("job" -> "My job -5"),
            -4L -> Map("job" -> "My job -4"),
            -3L -> Map("job" -> "My job -3"),
            -2L -> Map("job" -> "My job -2"),
            -1L -> Map("job" -> "My job -1")
          )
        )
      )
    ).offset(5)
    val actual = input.offset(5)
    Stepless.assertEqualsWithMeta(context, actual, expected)
  }
}
