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

//import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.Uri
import com.fasterxml.jackson.databind.JsonNode
import com.netflix.atlas.core.db.SimpleStaticDatabase
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.graph.TryHeatMap.GraphResponse
import com.netflix.atlas.json.Json

import java.io.FileInputStream
import java.util.Map.Entry
import java.util.function.Consumer
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

  private val graphAssertions =
    new GraphAssertions(goldenDir, targetDir, (a, b) => assertEquals(a, b))

  private val db = {
    val f = "/Users/clarsen/Downloads/ptile_data2.json"
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

    // StaticDatabase.demo
    new SimpleStaticDatabase(timeseries.result(), ConfigFactory.load().getConfig("atlas.core.db"))
  }
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

  imageTest("my histo") {
    // "/api/v1/graph?&s=e-24h&e=2012-01-15T00:00&no_legend=1&q=name,requestLatency,:eq,(,percentile,),:by&tick_labels=off"
    "/api/v1/graph?q=name,ipc.server.call,:eq,(,percentile,),:by&tick_labels=off&no_legend=1"
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
