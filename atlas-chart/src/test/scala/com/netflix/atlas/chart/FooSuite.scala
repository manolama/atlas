package com.netflix.atlas.chart

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.util.GraphAssertions
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.Datapoint
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.IrregularSeries
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.TimeSeq
import munit.FunSuite

import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util

class FooSuite extends FunSuite {

  val start: Long = 1704067200000L
  val graphEngine: PngGraphEngine = new DefaultGraphEngine
  private val baseDir = SrcPath.forProject("atlas-chart")
  private val goldenDir = s"$baseDir/src/test/resources/graphengine/${getClass.getSimpleName}"
  private val targetDir = s"$baseDir/target/${getClass.getSimpleName}"

  private val graphAssertions =
    new GraphAssertions(goldenDir, targetDir, (a, b) => assertEquals(a, b))

  test("foo") {
    val line = LineDef(series1, groupByKeys = List("k1"))
    val plotDef = PlotDef(List(line), genericX = true)
    val graphDef = GraphDef(
      width = 480,
      startTime = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
      endTime = ZonedDateTime.of(2024, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
      plots = List(plotDef)
    )
    val name = "_FOO.png"
//    val json = JsonCodec.encode(graphDef)
//    assertEquals(graphDef.normalize, JsonCodec.decode(json).normalize)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, true)
  }

  def series1: IregTS = {
    IregTS(
      start,
      "some.series",
      Map("k1" -> "v1"),
      List(
        Map("job" -> "Batch job a", "ts" -> "1704068642000"),
        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
        Map("job" -> "Batch job c", "ts" -> "1704080642000"),
        Map("job" -> "Batch job d", "ts" -> "1704090907000"),
        Map("job" -> "Batch job e", "ts" -> "1704092439000")
      ),
      Array(
        1.0, 2.0, 3.0, 4.0, 5.0
      )
    )
  }
}

case class IregTS(
  start: Long,
  val label: String,
  val tags: Map[String, String],
  val meta: List[Map[String, String]],
  dps: Array[Double]
) extends IrregularSeries {

  override def datapoint(index: Long): Datapoint = {
    Datapoint(tags, index, dps(index.toInt))
  }

  override val data: TimeSeq = new TSeq
  override def id: ItemId = ???

  override def toString(): String = {
    val buf = new StringBuffer("BatchTimeSeries(\n")
      .append("  label = ")
      .append(label)
      .append(",\n")
      .append("  tags = ")
      .append(tags)
      .append(",\n")
      .append("  meta = \n")
    meta.foreach { m =>
      buf.append("      ").append(m).append(",\n")
    }
    buf
      .append("  dps = ")
      .append(util.Arrays.toString(dps))
      .append("\n")
      .append(")")
      .toString
  }

  class TSeq extends TimeSeq {

    override def apply(timestamp: Long): Double = dps(timestamp.toInt)
    override def dsType: DsType = DsType.Gauge
    override def step: Long = 1

    override def foreach(s: Long, e: Long)(f: (Long, Double) => Unit): Unit = {
      for (i <- 0 until dps.size) {
        f(i, dps(i))
      }
    }
  }
}
