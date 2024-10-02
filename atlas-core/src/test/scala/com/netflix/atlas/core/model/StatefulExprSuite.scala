package com.netflix.atlas.core.model

import com.netflix.atlas.core.model.Stepless.assertEqualsWithMetaFunc
import com.netflix.atlas.core.model.Stepless.steplessContext
import munit.FunSuite

class StatefulExprSuite extends FunSuite {

  test("delay: stepless") {
    val context = steplessContext(14)
    val input =
      Stepless.ts(context, 1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    val trend = StatefulExpr.Delay(DataExpr.Sum(Query.Equal("name", "cpu")), 3)
    val results = trend.eval(context, List(input)).data.head
    val expected = Stepless.ts(
      context,
      Double.NaN,
      Double.NaN,
      Double.NaN,
      1.0,
      1.5,
      1.6,
      1.7,
      1.4,
      1.3,
      1.2,
      1.0,
      0.0,
      0.0,
      1.0
    )
    assertEqualsWithMetaFunc(context, results, expected)
  }

  test("rolling-count: stepless") {
    val context = steplessContext(14)
    val input =
      Stepless.ts(context, 1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    val trend = StatefulExpr.RollingCount(DataExpr.Sum(Query.Equal("name", "cpu")), 3)
    val results = trend.eval(context, List(input)).data.head
    val expected =
      Stepless.ts(context, 1.0, 2.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 3.0)
    assertEqualsWithMetaFunc(context, results, expected)
  }

  test("rolling-min: stepless") {
    val context = steplessContext(14)
    val input =
      Stepless.ts(context, 1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    val trend = StatefulExpr.RollingMin(DataExpr.Sum(Query.Equal("name", "cpu")), 5)
    val results = trend.eval(context, List(input)).data.head
    val expected =
      Stepless.ts(context, 1.0, 1.0, 1.0, 1.0, 1.0, 1.3, 1.2, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    assertEqualsWithMetaFunc(context, results, expected)
  }

  test("rolling-max: stepless") {
    val context = steplessContext(14)
    val input =
      Stepless.ts(context, 1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    val trend = StatefulExpr.RollingMax(DataExpr.Sum(Query.Equal("name", "cpu")), 5)
    val results = trend.eval(context, List(input)).data.head
    val expected =
      Stepless.ts(context, 1.0, 1.5, 1.6, 1.7, 1.7, 1.7, 1.7, 1.7, 1.4, 1.3, 1.2, 1.1, 1.2, 1.2)
    assertEqualsWithMetaFunc(context, results, expected)
  }

  test("rolling-mean: stepless") {
    val context = steplessContext(14)
    val input =
      Stepless.ts(context, 1.0, 1.5, 1.6, 1.7, 1.4, 1.3, 1.2, 1.0, 0.0, 0.0, 1.0, 1.1, 1.2, 1.2)
    val trend = StatefulExpr.RollingMean(DataExpr.Sum(Query.Equal("name", "cpu")), 3, 2)
    val results = trend.eval(context, List(input)).data.head
    val expected =
      Stepless.ts(
        context,
        Double.NaN,
        1.25,
        1.36666,
        1.59999,
        1.56666,
        1.46666,
        1.29999,
        1.16666,
        0.73333,
        0.33333,
        0.33333,
        0.69999,
        1.09999,
        1.16666
      )
    assertEqualsWithMetaFunc(context, results, expected)
  }
}
