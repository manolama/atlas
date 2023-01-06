package com.netflix.atlas.chart.graphics

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import java.time.Instant

class BasicHeatMapSuite extends FunSuite {

  test("foo") {
    val dps = new Array[Double](59)
    val ts = TimeSeries(
      Map.empty,
      new ArrayTimeSeq(DsType.Gauge, 1672819200000L, 60_000, dps)
    )
    val plotDef = PlotDef(List(LineDef(ts)))
    val graphDef = GraphDef(
      List(plotDef),
      Instant.ofEpochMilli(1672819200000L),
      Instant.ofEpochMilli(1672819200000L + (60_000 * 60))
    )
    val yaxis = LeftValueAxis(plotDef, graphDef.theme.axis, dps.min, dps.max)
    val timeAxis = TimeAxis(
      Style(color = graphDef.theme.axis.line.color),
      graphDef.startTime.toEpochMilli,
      graphDef.endTime.toEpochMilli,
      graphDef.step,
      graphDef.timezone,
      40
    )
    val heatmap = new BasicHeatMap(
      graphDef,
      plotDef,
      yaxis,
      timeAxis,
      x1 = 0,
      y1 = 0,
      x2 = 400,
      chartEnd = 200
    )

  }

}
