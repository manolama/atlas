package com.netflix.atlas.eval.stream

import com.netflix.atlas.eval.model.TimeSeriesMessage
import com.netflix.atlas.eval.stream.Evaluator.{DataSource, DataSources, DatapointGroup, MessageEnvelope}
import com.netflix.atlas.pekko.StreamOps
import com.netflix.atlas.pekko.StreamOps.SourceQueue
import com.netflix.spectator.api.{NoopRegistry, Registry}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.reactivestreams.{Processor, Subscriber, Subscription}

import java.time.Duration
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object LWCScratch {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val system = ActorSystem("atlas-lwc", config)
    implicit val mat = Materializer(system)

    val uri = args(0)

    val registry: Registry = new NoopRegistry
    val ds = new DataSource("cltest", Duration.ofMinutes(1), uri)
    val dss = DataSources.of(ds)
    val eval = new Evaluator(config, registry, system)
    val f = Source
      .tick(0.second, 10.day, NotUsed)
      .map(_ => dss)
      .via(eval.createStreamsFlow)
      .map { msg =>

        msg.message() match {
          case _: TimeSeriesMessage =>
            System.out.println(msg.id() + " => " + msg.message())
            System.err.println("-------------------------------------------------------------")
          case _ =>
        }
      }
      .run()

    Await.result(f, 1.hour)
  }
}
