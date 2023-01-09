/*
 * Copyright 2014-2022 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.eval.graph

import akka.http.scaladsl.model.Uri
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktIdx
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktNanos
import com.netflix.atlas.chart.graphics.PercentileHeatMap.bktSeconds
import com.netflix.atlas.chart.graphics.HeatMapTimerValueAxis
import com.netflix.atlas.chart.graphics.Scales
import com.netflix.atlas.chart.graphics.Style
import com.netflix.atlas.chart.graphics.Styles
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.db.SimpleStaticDatabase
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.graph.TryHeatMap.GraphResponse
import com.netflix.atlas.json.Json
import com.netflix.spectator.api.histogram.PercentileBuckets

import java.io.FileInputStream
import java.util.Map.Entry
import java.util.function.Consumer
import scala.jdk.CollectionConverters._
import scala.math.log10
//import akka.http.scaladsl.model.headers.Host
import com.netflix.atlas.chart.util.GraphAssertions
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.core.db.StaticDatabase
import com.netflix.atlas.core.util.Hash
import com.netflix.atlas.core.util.Strings
//import com.netflix.atlas.json.Json
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import java.io.File
import java.io.FileOutputStream

class TryHeatMap extends FunSuite {

  private val bless = false

  // SBT working directory gets updated with fork to be the dir for the project
  private val baseDir = SrcPath.forProject("atlas-eval")
  private val goldenDir = s"$baseDir/src/test/resources/graph/${getClass.getSimpleName}"
  private val targetDir = s"$baseDir/target/${getClass.getSimpleName}"

  private var db: Database = null
  private val grapher = Grapher(ConfigFactory.load())

  def imageTest(name: String)(uri: => String): Unit = {
    test(name) {
      val fname = Strings.zeroPad(Hash.sha1bytes(name), 40).substring(0, 8) + ".png"
      System.out.println(s"FNAME ${fname}")
      val result = grapher.evalAndRender(Uri(uri), db)
      System.out.println(s"Res ${result}")
      val image = PngImage(result.data)
      val file = new File(new File("/tmp"), "testpng.png")
      file.getParentFile.mkdirs()
      val stream = new FileOutputStream(file)
      image.write(stream)
      stream.close()

      // graphAssertions.assertEquals(image, fname, bless)
    }
  }

  def getDB(custom: Boolean = false): Database = {
    if (custom) {
      val f = "/Users/clarsen/Downloads/ptile_data4.json"
      val json = Json.decode[GraphResponse](new FileInputStream(f))
      val w = 3600 * 3
      var time = System.currentTimeMillis() - (w * 1000)
      time = time - (time % 60_000)
      val timeseries = List.newBuilder[TimeSeries]
      val seriesArrays = new Array[Array[Double]](json.metrics.length)
      for (i <- 0 until json.metrics.length) {
        seriesArrays(i) = new Array[Double](w / 60)
      }

      json.values.zipWithIndex.foreach { tuple =>
        val timeIndex = tuple._2
        tuple._1.zipWithIndex.foreach { arrayTuple =>
          val tsIndex = arrayTuple._2
          seriesArrays(tsIndex)(timeIndex) = arrayTuple._1
        }
      }
      seriesArrays.zipWithIndex.foreach { t =>
        timeseries += TimeSeries(
          json.metrics(t._2),
          new ArrayTimeSeq(DsType.Gauge, time, 60_000, t._1)
        )
      }

      var ts = timeseries.result()

      val keys = List(
        "T0001",
        "T0036",
        "T0037",
        "T0038",
        "T0039",
        "T003A",
        "T003B",
        "T003C",
        "T003D",
        "T003E", // this sucker causes a peak value at 7 cells from the end to disappear!!!
        "T003F",
        "T0040",
        "T0041",
        "T0042",
        "T0043",
        "T0044",
        "T0045",
        "T0046",
        "T0047",
        "T0048",
        "T0049",
        "T004A",
        "T004B",
        "T004C",
        "T004D",
        "T004E",
        "T004F",
        "T0050",
        "T0051",
        "T0052",
        "T0053",
        "T0054",
        "T0055",
        "T0056",
        "T0057",
        "T0058",
        "T0059",
        "T005A",
        "T005B",
        "T005C",
        "T005D",
        "T005E",
        "T005F",
        "T0060",
        "T0061",
        "T0062",
        "T0063",
        "T0064",
        "T0065",
        "T0066",
        "T0067",
        "T0068",
        "T0069",
        "T006A",
        "T006B",
        "T006C",
        "T006D",
        "T006E",
        "T006F",
        "T0070",
        "T0071",
        "T0072",
        "T0073",
        "T0074",
        "T0075",
        "T0076",
        "T0077",
        "T0078",
        "T0079",
        "T007A",
        "T007B",
        "T007C",
        "T007D",
        "T007E",
        "T007F",
        "T0080",
        "T0081",
        "T0082",
        "T0083",
        "T0084",
        "T0085",
        "T0086",
        "T0087",
        "T0088",
        "T0089",
        "T008A",
        "T008B",
        "T008C",
        "T008D",
        "T008E",
        "T008F",
        "T0090",
        "T0091",
        "T0092",
        "T0093",
        "T0094",
        "T0095",
        "T0096",
        "T0097",
        "T0098",
        "T0099",
        "T009A",
        "T009B",
        "T009C",
        "T009D",
        "T009E",
        "T009F",
        "T00A0"
      ).asJava
      if (false) {
        ts = ts.filter { t =>
          keys.contains(t.tags("percentile"))
        }
      }

      new SimpleStaticDatabase(ts, ConfigFactory.load().getConfig("atlas.core.db"))
    } else {
      StaticDatabase.demo
    }
  }

  // GOOD tests
  /*imageTest("my histo") {
    db = getDB(true)
    // "/api/v1/graph?&s=e-24h&e=2012-01-15T00:00&no_legend=1&q=name,requestLatency,:eq,(,percentile,),:by&tick_labels=off"
    // "/api/v1/graph?q=name,ipc.server.call,:eq,(,percentile,),:by&no_legend=1&w=1296&h=400"
    // "/api/v1/graph?q=name,ipc.server.call,:eq,(,percentile,),:by&no_legend=1&w=1296&h=400&tz=UTC&tz=US/Pacific&title=IPC%20Server%20Call%20Time"
    // "/api/v1/graph?q=name,ipc.server.call,:eq,:percentile_heatmap"

    // "/api/v1/graph?q=name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,percentile,),:by,:per-step,:heatmap,bluegreen,:palette&scale=log&w=1296&h=400"

    // DEBUG PTILES
    "/api/v1/graph?q=" +
      "name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,percentile,),:by,:per-step,:heatmap,My%20Heatmap,:legend," + // "blues,:palette," + //"fcba03,:color," + // "bluegreen,:palette," +
      "name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,50,),:percentiles," + // "ff0000,:color,2,:lw," +
      "name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,99.99,),:percentiles," + // "c203fc,:color,2,:lw," +
      "name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,99.999999999,),:percentiles," + // "033dfc,:color,2,:lw," +
      "&w=1296&h=600" +
      "&scale=percentile" +
      "&heatmap_scale=log" +
      // "&heatmap_legend=My%20Nifty%20Percentile%20map"
      "&heatmap_palette=colors:1a9850,91cf60,d9ef8b,fee08b,fc8d59,d73027"
//    "&u=17.5"
//      "&l=2.45&u=2.92"

    // woot, works with y axis!
    // "/api/v1/graph?q=name,ipc.server.call,:eq,:percentile_heatmap,name,ipc.server.call,:eq,4,:lw,1,:axis,&w=1296&h=400"

    // "/api/v1/graph?q=secondOfDay,:time,:heatmap,blues,:palette,secondOfDay,:time,1,:axis,ff0000,:color&w=600&h=400&e=1671137340000&s=e-3h"
    // "/api/v1/graph?q=name,sps,:eq,(,nf.cluster,),:by,:heatmap,bluegreen,:palette,name,sps,:eq,(,nf.cluster,),:by,ff0000,:color&w=1296&h=400&no_legend_stats=1"
    // "/api/v1/graph?q=name,sps,:eq,(,nf.cluster,),:by,:heatmap,bluegreen,:palette,name,sps,:eq,(,nf.cluster,),:by&w=1296&h=400&no_legend_stats=1"

    // heatmap, lines, heatmap same axis
    // "/api/v1/graph?q=name,sps,:eq,(,nf.cluster,),:by,:heatmap,bluegreen,name,sps,:eq,(,nf.cluster,),:by,ff0000,:color:palette,name,sps,:eq,(,nf.cluster,),:by,:heatmap,greens,:palette&w=1296&h=400"

    // two heatmaps, diff queries back to back
    // "/api/v1/graph?q=name,sps,:eq,(,nf.cluster,),:by,10,:mul,:heatmap,bluegreen,:palette,name,sps,:eq,(,nf.cluster,),:by,:heatmap,greens,:palette&w=1296&h=400"

    // two heatmaps, diff axis. Buggy but users shouldn't do this.
    // "/api/v1/graph?q=name,sps,:eq,(,nf.cluster,),:by,16,:mul,:heatmap,bluegreen,:palette,name,sps,:eq,(,nf.cluster,),:by,:heatmap,1,:axis,greens,:palette&w=1296&h=400"

    // TODO - in this case, the graph comes out as a line again just like it didn't have a heatmap.
    // "/api/v1/graph?q=42,:heatmap"
  }*/

  imageTest("histos") {
    db = getDB(false)
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,requestLatency,:eq,(,percentile,),:by" +
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,ipc.server.call,:eq,(,percentile,),:by&no_legend=1&w=1296&h=400"
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,ipc.server.call,:eq,(,percentile,),:by&no_legend=1&w=1296&h=400&tz=UTC&tz=US/Pacific&title=IPC%20Server%20Call%20Time"
//    "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=" +
//      "name,requestLatency,:eq,(,percentile,),:by,:per-step,:heatmap," +
//      // "name,requestLatency,:eq,(,percentile,),:by,:per-step,1,:axis" +
//      // "name,requestLatency,:eq,(,50,99.9,),:percentiles" +
//      "name,requestLatency,:eq," + "1,:axis," +
//      "&scale=percentile" +
//      "&tick_labels=duration" +
//      "&heatmap_scale=log"

    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,percentile,),:by,:per-step,:heatmap,bluegreen,:palette&scale=log&w=1296&h=400"

    // DEBUG PTILES
    // woot, works with y axis!
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,ipc.server.call,:eq,:percentile_heatmap,name,ipc.server.call,:eq,4,:lw,1,:axis,&w=1296&h=400"

    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=secondOfDay,:time,:heatmap,blues,:palette,secondOfDay,:time,1,:axis,ff0000,:color&w=600&h=400&e=1671137340000&s=e-3h"
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:heatmap,bluegreen,:palette,name,sps,:eq,(,nf.cluster,),:by,ff0000,:color&w=1296&h=400&no_legend_stats=1"
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:heatmap,bluegreen,:palette,name,sps,:eq,(,nf.cluster,),:by&w=1296&h=400&no_legend_stats=1"
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:heatmap,bluegreen,:palette,name,sps,:eq,(,nf.cluster,),:by&w=1296&h=400&scale=percentile"

    // ptile scale
//    "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,requestLatency,:eq," +
//      "&scale=percentile"

    // heatmap, lines, heatmap same axis
    "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,sps,:eq,(,nf.cluster,),:by,:heatmap,Whohohohoh,:legend," +
      "name,sps,:eq,(,nf.cluster,),:by,"

    // line
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=42,:heatmap,45,:heatmap,48,:heatmap,99,:heatmap"

    // two heatmaps, diff queries back to back
//    "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,sps,:eq,(,nf.cluster,),:by,10,:mul,:heatmap,bluegreen,:palette," +
//      "name,sps,:eq,(,nf.cluster,),:by,:heatmap,greens,:palette&w=1296&h=400"

    // two heatmaps, diff axis. Works fine.
//    "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=name,sps,:eq,(,nf.cluster,),:by,16,:mul,:heatmap,bluegreen,:palette," +
//    "name,sps,:eq,(,nf.cluster,),:by,16,:mul," +
//      "name,sps,:eq,(,nf.cluster,),:by,:heatmap,1,:axis,greens,:palette," +
//    "name,sps,:eq,(,nf.cluster,),:by,1,:axis," +
//      "&w=1296&h=400"

    // TODO - in this case, the graph comes out as a line again just like it didn't have a heatmap.
    // "/api/v1/graph?s=e-24h&e=2012-01-15T00:00&q=42,:heatmap"

  }

  test("jsonv2 maybe?") {
    db = getDB(true)
    val uri =
      "/api/v1/graph?q=name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,percentile,),:by,:per-step,:heatmap,bluegreen,:palette," +
        "name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,50,),:percentiles,ff0000,:color,2,:lw," +
        "name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,99.99,),:percentiles,c203fc,:color,2,:lw," +
        "name,ipc.server.call,:eq,statistic,percentile,:eq,:and,(,99.999999999,),:percentiles,033dfc,:color,2,:lw," +
        "&w=1296&h=600" +
        "&scale=percentile" +
        "&hints=no-image" +
        "&format=v2.json"
    val result = grapher.evalAndRender(Uri(uri), db)
    System.out.println(result.dataString)
  }

  def scaleBackLOG(
    d1: Double,
    d2: Double,
    r1: Int,
    r2: Int,
    x: Int
  ): Double = {
//    Math
//      .pow(10, d1) + ((Math.pow(10, x) - r1) * ((log10(d2) - log10(d1)) / (r2 - r1)))
//    ((log10(d1) * (Math.exp(x) - r2)) + (log10(d2) * (r1 - Math.exp(x)))) / (r1 - r2)
//    Math.exp(
//      (d1 * r1 - r1 * log10(d1) - d1 * r2 + x * log10(d1) + r1 * log10(d2) - x * log10(
//        d2
//      )) / (r1 - r2)
//    )

    // wolfram without parens
//    d1 * Math.exp(
//      Math.pow(r1, 2) * (-Scales.log10(d1)) + r1 * r2 * Scales.log10(d1) + r1 * x * Scales.log10(
//        d1
//      ) - r2 * x * Scales.log10(d1) + Math.pow(r1, 2) * Scales.log10(d2) - r1 * r2 * Scales.log10(
//        d2
//      ) - r1 * x * Scales.log10(d2) + r2 * x * Scales.log10(d2)
//    )

    // what I thought it should be.
//    Math.pow(10, ((x - r1) * (r2 - r1) * (Scales.log10(d2) - Scales.log10(d1))) + Scales.log10(d1))

    // wolfram WITH parens
//    Math.exp(
//      (-r2 * Scales.log10(d1) + x * Scales.log10(d1) + r1 * Scales.log10(d2) - x * Scales.log10(
//        d2
//      )) / (r1 - r2)
//    )

    // try scale back linear
    Math.pow(10, scaleBackLinear(Scales.log10(d1), Scales.log10(d2), r1, r2, x))
  }

  def scaleBackLinear(
    d1: Double,
    d2: Double,
    r1: Int,
    r2: Int,
    x: Int
  ): Double = {
    val pixelSpan = (d2 - d1) / (r2 - r1)
    ((x - r1) * pixelSpan) + d1
  }

  test("scale plus 1") {
    var d1 = 1.0
    var d2 = 321000.0
    var r1 = 0
    var r2 = 3
    // var scale = Scales.factory(Scale.LOGARITHMIC)(d1, d2 + 1, r1, r2 + 1)
    var scale = Scales.logarithmic(d1, d2 + 1, r1, r2)

    val reals = List(20, 40, 200, 3000, 25000, 320000, 320001, 321000 - 1, 321000)
    reals.foreach { r =>
      var s = scale(r)
      if (s >= r2) {
        System.out.println(s"@@@@@ WARNING! @ ${r} gave ${s}")
        s -= 1
      }
      System.out.println(s"${r} scaled: ${s}")
    }
  }

  test("scale plus fraction") {
    // NOPE - doesn't work without stringifying and man that gets ugly. Just
    // switch to linear for sub 1 d2s.
    var d1 = 0.053
    var d2 = 0.01
    var r1 = 0
    var r2 = 3
    // var scale = Scales.factory(Scale.LOGARITHMIC)(d1, d2 + 1, r1, r2 + 1)
    var scale = Scales.logarithmic(d1, d2 + 1, r1, r2)

    val reals = List(0.053, 0.07, 0.08, 0.081)
    reals.foreach { r =>
      var s = scale(r)
      if (s >= r2) {
        System.out.println(s"@@@@@ WARNING! @ ${r} gave ${s}")
        s -= 1
      }
      System.out.println(s"${r} scaled: ${s}")
    }

    val exp = Math.getExponent(d2)
    val mant = d2 / Math.pow(2, exp)
    val isthisit = mant * Math.pow(2, exp)
    val shift = d2 * Math.pow(10, Math.abs(exp))
    System.out.println(
      s"MAnt for ${d2} is ${mant}  (exp = ${exp}) and ? ${isthisit} or Shift ${shift}"
    )

    val b = new java.math.BigDecimal(d2)
    val inc = java.math.BigDecimal.valueOf(1).scaleByPowerOfTen(-b.scale)
    val out = b.add(inc).stripTrailingZeros().doubleValue()
    System.out.println(String.format("ORG: %.5f", d2))
    System.out.println(String.format("INC: %.5f", out))
  }

  test("scale backwards is ___?") {
//    val d1 = 1.0
//    val d2 = 4.0
//    val r1 = 0
//    val r2 = 6

    var d1 = 1.0
    var d2 = 321000.0
    var r1 = 0
    var r2 = 6

    val v = 0.0
    val x = ((log10(v) - d1) / ((log10(d2) - log10(d1)) / (r2 - r1))) + r1

    var scale = Scales.factory(Scale.LOGARITHMIC)(d1, d2, r1, r2)
    // val scale = Scales.factory(Scale.LINEAR)(d1, d2, r1, r2)

    for (i <- d1.toInt to d2.toInt) {
      // System.out.println(s"maxmin ${i} => ${r2 - scale(i)}")
    }
//    System.out.println("--------")
//    for (i <- d1.toInt to d2.toInt) {
//      val v = scale(i)
//      // val sb = d2 - scaleBackLinear(d1, d2, r1, r2, v)
//      val sb = d2 - scaleBackLOG(d1, d2, r1, r2, v)
//      System.out.println(s"v ${i} => ${v} RevScale: => ${sb}")
//    }

    System.out.println("----------------------------")
    val reals = List(20, 200, 3000, 25000, 320000, 320001, 321000 - 1, 321000)
    // val reals = List(1, 64200, 64200 * 2, 64200 * 3, 320000)
    d2 = 321000
    scale = Scales.factory(Scale.LOGARITHMIC)(d1, d2 + 1, r1, r2 + 1)
    // scale = Scales.factory(Scale.LINEAR)(d1, d2, r1, r2)

    reals.foreach { r =>
      var s = scale(r)
      if (s >= r2) {
        s -= 1
      }
      val sb = d2 - scaleBackLOG(d1, d2 + 1, r1, r2, s)
      // val sb = d2 - scaleBackLinear(d1, d2, r1, r2, s)
      System.out.println(s"${r} scaled: ${s}  Back: ${sb.toInt}")
    }
  }

//  test("such great heights") {
//    val series = 165
//    val lines = series + 1
//    val height = 405
//
//    val dpHeight = height.toDouble / lines
//    System.out.println(dpHeight)
//
////    var last = 0.0
////    var pixels = 0
////    for (_ <- 0 until lines) {
////      val next = last + dpHeight
////      val h = (Math.round(next) - Math.round(last)).toInt
////      val off = Math.round(last).toInt
////      pixels += h
////      //System.out.println(s"H ${h} @ Off ${off}")
////      last = next
////    }
////    System.out.println(s"Remainder: ${height - pixels} pixels")
//
//    // top down
//    var prev = height.toDouble
//    for (_ <- 0 until lines) {
//      val next = prev - dpHeight
//      val h = (Math.round(prev) - Math.round(next)).toInt
//      val offset = Math.round(prev).toInt - h
//      prev = next
//      System.out.println(s"Offset ${offset}")
//    }
//    System.out.println(s"Remainder: ${prev}")
//
//  }

  test("the bucketeer....") {

    /**
      * BUCKET IDX: 158  seconds: 68.71947673599999
      *  BUCKET IDX: 159  seconds: 91.62596898100001
      *  BUCKET IDX: 160  seconds: 114.532461226
      */
    val bktMax = 274
    var seconds = bktSeconds(bktMax)
    System.out.println(s"SECONDS ${seconds} for ${bktMax}")

    seconds = 5.759865526446057e9
    val idx = PercentileBuckets.indexOf(seconds.toLong * 1000 * 1000 * 1000)
    System.out.println(s"To bkt Idx: ${idx}")
  }

//  test("bucket boundaries?") {
//    val counts = new Array[Long](PercentileBuckets.length())
//    counts(17) += 15
//
//    System.out.println("Nanos for bkt 5: " + PercentileBuckets.get(5))
//    System.out.println(
//      PercentileBuckets.indexOf(20) + " => " + PercentileBuckets.get(PercentileBuckets.indexOf(20))
//    )
//    System.out.println(
//      PercentileBuckets.indexOf(21) + " => " + PercentileBuckets.get(PercentileBuckets.indexOf(21))
//    )
//    System.out.println(
//      PercentileBuckets.indexOf(25) + " => " + PercentileBuckets.get(PercentileBuckets.indexOf(25))
//    )
//    System.out.println(
//      PercentileBuckets.indexOf(26) + " => " + PercentileBuckets.get(PercentileBuckets.indexOf(26))
//    )
//    System.out.println(
//      PercentileBuckets.indexOf(27) + " => " + PercentileBuckets.get(PercentileBuckets.indexOf(27))
//    )
//
//    System.out.println(s"P: ${PercentileBuckets.percentile(counts, 99.9999)}")
//
//  }

  test("scale") {
//    val axis = HeatMapTimerValueAxis(
//      PlotDef(List.empty),
//      Styles(Style(), Style(), Style()),
//      1.9999999999999997e-9,
//      114.532461226
//    )
//    axis.ticks(5, 405)
    System.out.println(
      s"BKT For the first.... ${PercentileBuckets.indexOf((1.9999999999999997e-9 * 1000 * 1000 * 1000).toInt)}"
    )
    val scaler = Scales.percentile(1.9999999999999997e-9, 114.532461226, 5, 405)

//    for (i <- 0 to 160) {
//      System.out.println(s"${bktSeconds(i)} -> ${scaler(bktSeconds(i))}")
//    }
    System.out.println("--------------")
    // alignment
    System.out.println(s"${63} -> ${scaler(63)}")
    System.out.println(s"${75} -> ${scaler(75)}")
    System.out.println(s"${91} -> ${scaler(91)}")
    System.out.println(s"${92} -> ${scaler(92)}")

    System.out.println(s"Bucket 0 = ${PercentileBuckets.get(0)}")
    System.out.println(s"Bucket 1 = ${PercentileBuckets.get(1)}")
  }

  test("fudgefactor") {
    val realBoundary = 2.147483647
    val test = 2.4

    var idx = bktIdx((realBoundary * 1000 * 1000 * 1000).toLong)
    var givenS = bktNanos(idx - 1)
    var matched = (realBoundary * 1000 * 1000 * 1000).toLong == givenS
    System.out.println(s"MATCHED? ${matched}")

    idx = bktIdx((test * 1000 * 1000 * 1000).toLong)
    givenS = bktNanos(idx)
    matched = (test * 1000 * 1000 * 1000).toLong == givenS
    System.out.println(s"MATCHED? ${matched}")
  }

}

object TryHeatMap {

  case class GraphResponse(
    start: Long,
    step: Long,
    legend: List[String],
    metrics: List[Map[String, String]],
    values: Array[Array[Double]],
    notices: List[String],
    explain: Map[String, Long]
  )
}
