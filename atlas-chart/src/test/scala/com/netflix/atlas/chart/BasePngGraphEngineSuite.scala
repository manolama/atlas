package com.netflix.atlas.chart

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.util.GraphAssertions
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.json.Json
import munit.FunSuite

import java.awt.Color
import java.time.Duration
import java.time.ZoneOffset
import java.time.ZonedDateTime
import scala.util.Failure
import scala.util.Try
import scala.util.Using

class BasePngGraphEngineSuite extends FunSuite {

  protected val baseDir = SrcPath.forProject("atlas-chart")
  protected val goldenDir = s"$baseDir/src/test/resources/graphengine/${getClass.getSimpleName}"
  protected val targetDir = s"$baseDir/target/${getClass.getSimpleName}"

  protected val graphAssertions =
    new GraphAssertions(goldenDir, targetDir, (a, b) => assertEquals(a, b))

  /** Whether to create/overwrite the images for validation. */
  protected val bless = true

  override def afterAll(): Unit = {
    graphAssertions.generateReport(getClass)
  }

  def step: Long = 60000

  def graphEngine: PngGraphEngine = new DefaultGraphEngine

  def label(vs: LineDef*): List[LineDef] = label(0, Palette.default, vs*)

  def label(offset: Int, p: Palette, vs: LineDef*): List[LineDef] = {
    vs.toList.zipWithIndex.map {
      case (v, i) =>
        val c = p.withAlpha(v.color.getAlpha).colors(i + offset)
        v.copy(data = v.data.withLabel(i.toString), color = c)
    }
  }

  def load(resource: String): GraphDef = {
    Using.resource(Streams.resource(resource)) { in =>
      Json.decode[GraphData](in).toGraphDef
    }
  }

  def loadV2(resource: String): GraphDef = {
    Using.resource(Streams.gzip(Streams.resource(resource))) { in =>
      JsonCodec.decode(in)
    }
  }

  def checkImpl(name: String, graphDef: GraphDef): Unit = {
    val json = JsonCodec.encode(graphDef)
    assertEquals(graphDef.normalize, JsonCodec.decode(json).normalize)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, bless)
  }

  def check(name: String, graphDef: GraphDef): Unit = {
    val c1 = Try { checkImpl(name, graphDef) }
    val c2 = Try { checkImpl(s"dark_$name", graphDef.copy(themeName = "dark")) }
    (c1, c2) match {
      case (_, Failure(e)) => throw e
      case (Failure(e), _) => throw e
      case _               =>
    }
  }

  def makeTranslucent(c: Color): Color = {
    new Color(c.getRed, c.getGreen, c.getBlue, 75)
  }

  // --------- Helper functions for creating test data ----------
  def constant(v: Double): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
  }

  def wave(min: Double, max: Double, wavelength: Duration): TimeSeries = {
    val lambda = 2 * scala.math.Pi / wavelength.toMillis

    def f(t: Long): Double = {
      val amp = (max - min) / 2.0
      val yoffset = min + amp
      amp * scala.math.sin(t * lambda) + yoffset
    }
    TimeSeries(Map("name" -> "wave"), new FunctionTimeSeq(DsType.Gauge, step, f))
  }

  def interval(ts1: TimeSeries, ts2: TimeSeries, s: Long, e: Long): TimeSeries = {

    def f(t: Long): Double = {
      val ts = if (t >= s && t < e) ts2 else ts1
      ts.data(t)
    }
    TimeSeries(Map("name" -> "interval"), new FunctionTimeSeq(DsType.Gauge, step, f))
  }

  def finegrainWave(min: Int, max: Int, hours: Int): TimeSeries = {
    wave(min, max, Duration.ofHours(hours))
  }

  def finegrainSeriesDef(min: Int, max: Int, hours: Int): LineDef = {
    LineDef(finegrainWave(min, max, hours))
  }

  def simpleWave(min: Double, max: Double): TimeSeries = {
    wave(min, max, Duration.ofDays(1))
  }

  def simpleWave(max: Double): TimeSeries = {
    simpleWave(0, max)
  }

  def simpleSeriesDef(min: Double, max: Double): LineDef = {
    LineDef(simpleWave(min, max), query = Some(s"$min,$max"))
  }

  def simpleSeriesDef(max: Double): LineDef = {
    simpleSeriesDef(0, max)
  }

  def outageSeriesDef(max: Int): LineDef = {
    val start1 = ZonedDateTime.of(2012, 1, 1, 5, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end1 = ZonedDateTime.of(2012, 1, 1, 6, 38, 0, 0, ZoneOffset.UTC).toInstant

    val start2 = ZonedDateTime.of(2012, 1, 1, 7, 4, 0, 0, ZoneOffset.UTC).toInstant
    val end2 = ZonedDateTime.of(2012, 1, 1, 7, 5, 0, 0, ZoneOffset.UTC).toInstant

    val bad = constant(0)
    val normal = interval(simpleWave(max), bad, start1.toEpochMilli, end1.toEpochMilli)
    LineDef(interval(normal, bad, start2.toEpochMilli, end2.toEpochMilli))
  }

  def constantSeriesDef(value: Double): LineDef = {
    LineDef(constant(value))
  }
}
