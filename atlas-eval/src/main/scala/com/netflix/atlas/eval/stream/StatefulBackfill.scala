package com.netflix.atlas.eval.stream

import com.netflix.atlas.chart.JsonCodec
import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.stream.Evaluator.DataSource
import com.netflix.atlas.eval.stream.HostSource.unzipIfNeeded
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.PekkoHttpClient
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.Uri.Query
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success

class StatefulBackfill(
  config: Config,
  registry: Registry,
  implicit val system: ActorSystem
) extends StrictLogging {

  def backfill(
    pekkoHttpClient: PekkoHttpClient,
    datasource: DataSource,
    timestamp: Long,
    count: Int,
    expressions: Seq[DataExpr]
  ): Map[DataExpr, List[TimeSeries]] = {
    val uri = Uri(datasource.uri())
    var queryParams = uri.query().toMap

    queryParams = queryParams + ("e"      -> (timestamp + datasource.step().toMillis).toString)
    queryParams = queryParams + ("format" -> "v2.json")
    // TODO - no-image flag
    // TODO maor to cleanup of the query params probably

    expressions.map { exp =>
      val start = timestamp - ((count + 1) * datasource.step().toMillis)
      var qp: Map[String, String] = queryParams + ("s" -> start.toString)
      qp = qp + ("q"                                   -> exp.toString)
      qp = qp + ("hints"                               -> "no-image")

      val updatedUri = uri.withQuery(Query(qp))
      System.err.println("Making a backfill call to " + updatedUri)
      val f = Source
        .single(HttpRequest(uri = updatedUri))
        // TODO - add some jitter
        .via(pekkoHttpClient.simpleFlow())
        .flatMapConcat {
          case Success(resp) =>
            unzipIfNeeded(resp)
              .map(_.utf8String)
              .map { body =>
                resp.status match {
                  case StatusCodes.OK =>
                    try {
                      val graphDef = JsonCodec.decode(body)
                      // System.err.println("Parsed: " + graphDef)
                      System.err.println(
                        "FROM Backend: " + graphDef.plots.head.lines.map(_.data).map(_.data)
                      )
                      exp -> graphDef.plots.head.lines.map(_.data)
                    } catch {
                      case e: Exception =>
                        logger.warn(
                          s"failed to backfill data for ${datasource.uri()}: ${e.getMessage}"
                        )
                        exp -> List.empty[TimeSeries]
                    }
                  case _ =>
                    logger.warn(s"failed to backfill data for ${datasource.uri()}: ${resp.status}")
                    exp -> List.empty[TimeSeries]
                }
              }
          case Failure(exception) =>
            // already handled retries. Suppress till the next interval
            Source.single(exp -> List.empty[TimeSeries])
        }
        .toMat(Sink.head)(Keep.right)
        .run()
      // TODO - error handling
      try {
        // TODO - !!!!!!!!!!!!!!!!!!!! NO WAITY!!! This'll block a Pekko thread.
        // But... it will be a pain in the arse to build a flow. Have to split the FinalExprEval.
        Await.result(f, 30.seconds)
      } catch {
        case e: Exception =>
          logger.warn(s"failed to backfill data for ${datasource.uri()}: ${e.getMessage}")
          exp -> List.empty[TimeSeries]
      }
    }.toMap

  }
}
