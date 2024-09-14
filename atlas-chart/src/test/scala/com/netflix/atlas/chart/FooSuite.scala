package com.netflix.atlas.chart

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.util.GraphAssertions
import com.netflix.atlas.chart.util.PngImage
import com.netflix.atlas.chart.util.SrcPath
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.BasicTimeSeries
import com.netflix.atlas.core.model.DatapointMeta
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.ItemId
import com.netflix.atlas.core.model.TimeSeq
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import java.awt.Color
import java.time.Instant
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
    val line2 = LineDef(series2, groupByKeys = List("k1"), color = Color.GREEN)
    val plotDef = PlotDef(List(line, line2))
    val graphDef = GraphDef(
      width = 480,
      startTime = Instant.ofEpochMilli(0),
      endTime = Instant.ofEpochMilli(5),
      step = 1,
      plots = List(plotDef),
      stepless = true
    )
    val name = "_FOO.png"
//    val json = JsonCodec.encode(graphDef)
//    assertEquals(graphDef.normalize, JsonCodec.decode(json).normalize)

    val image = PngImage(graphEngine.createImage(graphDef), Map.empty)
    graphAssertions.assertEquals(image, name, true)
  }

  def series1: TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(1.0, 2.0, 3.0, 4.0, 5.0))
    TSWithMeta(
      Map("k1" -> "v1"),
      "some.series",
      seq,
      List(
        Map(
          "job" -> "Batch job a",
          "ts" -> "1704068642000   asdfasdfadsfasadfadsfjlkajsdfslkjasdlkfjalskdfjfalskdjflkjasddlfkjasldkfjlkjasldkijfliasmldfimasldidmflaimsfdlimdslafimlasidmfloisdmf"
        ),
        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
        Map("job" -> "Batch job c", "ts" -> "1704080642000"),
        Map("job" -> "Batch job d", "ts" -> "1704090907000"),
        Map("job" -> "Batch job e", "ts" -> "1704092439000")
      )
    )
  }

  def series2: TimeSeries = {
    val seq = new ArrayTimeSeq(DsType.Gauge, 0, 1, Array(6.0, 4.0, 2.0, 5.0, 3.0))
    TSWithMeta(
      Map("k1" -> "v1"),
      "second.metric",
      seq,
      List(
        Map("job" -> "Batch job a", "ts" -> "1704068642000"),
        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
        Map("job" -> "Batch job c", "ts" -> "1704080642000"),
        Map("job" -> "Batch job d", "ts" -> "1704090907000"),
        Map("job" -> "Batch job e", "ts" -> "1704092439000")
      )
    )
  }

//  def series1: IregTS = {
//    IregTS(
//      start,
//      "some.series",
//      Map("k1" -> "v1"),
//      List(
//        Map("job" -> "Batch job a", "ts" -> "1704068642000"),
//        Map("job" -> "Batch job b", "ts" -> "1704074514000"),
//        Map("job" -> "Batch job c", "ts" -> "1704080642000"),
//        Map("job" -> "Batch job d", "ts" -> "1704090907000"),
//        Map("job" -> "Batch job e", "ts" -> "1704092439000")
//      ),
//      Array(
//        1.0, 2.0, 3.0, 4.0, 5.0
//      )
//    )
//  }
}

case class TSWithMeta(
  tags: Map[String, String],
  label: String,
  data: TimeSeq,
  meta: List[Map[String, String]]
) extends TimeSeries {

  /** Unique id based on the tags. */
  override def id: ItemId = ???

  override def datapointMeta(timestamp: Long): Option[DatapointMeta] = {
    Some(new MapMeta(meta(timestamp.toInt)))
  }
}

class MapMeta(map: Map[String, String]) extends DatapointMeta {

  override def keys: List[String] = map.keys.toList

  override def get(key: String): Option[String] = map.get(key)
}

//case class IregTS(
//  start: Long,
//  val label: String,
//  val tags: Map[String, String],
//  val meta: List[Map[String, String]],
//  dps: Array[Double]
//) extends IrregularSeries {
//
//  override def datapoint(index: Long): Datapoint = {
//    Datapoint(tags, index, dps(index.toInt))
//  }
//
//  override val data: TimeSeq = new TSeq
//  override def id: ItemId = ???
//
//  override def toString(): String = {
//    val buf = new StringBuffer("BatchTimeSeries(\n")
//      .append("  label = ")
//      .append(label)
//      .append(",\n")
//      .append("  tags = ")
//      .append(tags)
//      .append(",\n")
//      .append("  meta = \n")
//    meta.foreach { m =>
//      buf.append("      ").append(m).append(",\n")
//    }
//    buf
//      .append("  dps = ")
//      .append(util.Arrays.toString(dps))
//      .append("\n")
//      .append(")")
//      .toString
//  }
//
//  class TSeq extends TimeSeq {
//
//    override def apply(timestamp: Long): Double = dps(timestamp.toInt)
//    override def dsType: DsType = DsType.Gauge
//    override def step: Long = 1
//
//    override def foreach(s: Long, e: Long)(f: (Long, Double) => Unit): Unit = {
//      for (i <- 0 until dps.size) {
//        f(i, dps(i))
//      }
//    }
//  }
//}
