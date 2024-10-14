package com.netflix.atlas.core.model

import com.netflix.atlas.core.model.MetaWrapper.assertMeta
import munit.Assertions.assertEqualsDouble
import org.junit.Assert.assertTrue

object Stepless {

  //val s = 1704067200000L

  def steplessContext(n: Int): EvalContext = {
    val step = 1L

    val s = 0L
    val e = n * step
    val context = EvalContext(s, e, step, steplessLimit = Some(n))
    context
  }

  def ts(context: EvalContext, values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, context.start, context.step, values.toArray)
    MetaWrapper(TimeSeries(Map("name" -> "cpu", "node" -> "i-1"), seq, None))
  }

  def ts(start: Long, values: Double*): TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, start, 1, values.toArray)
    MetaWrapper(TimeSeries(Map("name" -> "cpu", "node" -> "i-1"), seq, None))
  }

  def assertEqualsWithMetaFunc(
    context: EvalContext,
    actual: TimeSeries,
    expected: TimeSeries
  ): Unit = {
    for (i <- context.start until context.end) {
      assertEqualsDouble(
        actual.data(i),
        expected.data(i),
        0.00001,
        s"values are not the same at index ${i}"
      )
      assertMeta(actual, i, List("job", s"My job {i}"))
    }
  }

  def assertEqualsWithMeta(
    context: EvalContext,
    actual: TimeSeries,
    expected: TimeSeries
  ): Unit = {
    for (i <- context.start until context.end) {
      assertEqualsDouble(
        actual.data(i),
        expected.data(i),
        0.00001,
        s"values are not the same at index ${i}"
      )
      System.out.println(s"Act: ${actual.data(i)}  Exp: ${expected.data(i)}")
      assertMeta(actual, i, expected.meta.get)
    }
  }

  def assertEqualsWithOutMeta(
    context: EvalContext,
    actual: TimeSeries,
    expected: TimeSeries
  ): Unit = {
    assertTrue(actual.meta.isEmpty)
    for (i <- context.start until context.end) {
      assertEqualsDouble(
        actual.data(i),
        expected.data(i),
        0.00001,
        s"values are not the same at index ${i}"
      )
    }
  }
}
