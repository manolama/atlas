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
package com.netflix.atlas.lwcapi

import akka.NotUsed
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorAttributes
import akka.stream.BoundedSourceQueue
import akka.stream.KillSwitches
import akka.stream.Materializer
import akka.stream.SharedKillSwitch
import akka.stream.Supervision
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.CustomDirectives._
import com.netflix.atlas.akka.DiagnosticMessage
import com.netflix.atlas.akka.StreamOps
import com.netflix.atlas.akka.StreamOps.SourceQueue
import com.netflix.atlas.akka.WebApi
import com.netflix.atlas.eval.model.LwcDataExpr
import com.netflix.atlas.eval.model.LwcHeartbeat
import com.netflix.atlas.eval.model.LwcMessages
import com.netflix.atlas.eval.model.LwcSubscription
import com.netflix.atlas.json.JsonSupport
import com.netflix.iep.NetflixEnvironment
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Inject
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

class SubscribeApi @Inject() (
  config: Config,
  registry: Registry,
  sm: StreamSubscriptionManager,
  splitter: ExpressionSplitter,
  implicit val materializer: Materializer
) extends WebApi
    with StrictLogging {

  import SubscribeApi._
  import com.netflix.atlas.akka.OpportunisticEC._

  private val queueSize = config.getInt("atlas.lwcapi.queue-size")
  private val batchSize = config.getInt("atlas.lwcapi.batch-size")

  private val evalsCounter = registry.counter("atlas.lwcapi.subscribe.count", "action", "subscribe")

  private val itemsCounter =
    registry.counter("atlas.lwcapi.subscribe.itemCount", "action", "subscribe")

  def routes: Route = {
    extractClientIP { addr =>
      endpointPathPrefix("api" / "v1" / "subscribe") {
        path(Remaining) { streamId =>
          val meta = StreamMetadata(streamId, addr.value)
          handleWebSocketMessages(createHandlerFlow(meta))
        }
      } ~
      endpointPathPrefix("api" / "v2" / "subscribe") {
        path(Remaining) { streamId =>
          val meta = StreamMetadata(streamId, addr.value)
          handleWebSocketMessages(createHandlerFlowV2(meta))
        }
      }
    }
  }

  /**
    * Drop any other connections that may already be using the same id
    */
  private def dropSameIdConnections(streamMeta: StreamMetadata): Unit = {
    val streamId = streamMeta.streamId
    sm.unregister(streamId).foreach { queue =>
      val msg = DiagnosticMessage.info(s"dropped: another connection is using id: $streamId")
      queue.offer(Seq(msg))
      queue.complete()
    }
  }

  /**
    * Uses text messages and sends each datapoint individually.
    */
  private def createHandlerFlow(streamMeta: StreamMetadata): Flow[Message, Message, Any] = {
    System.out.println("### Calling drop connections")
    dropSameIdConnections(streamMeta)
    val ks = KillSwitches.shared("WSS-KS")
    val flowComplete = new AtomicBoolean()

    System.out.println("### Creating handler flow")
    val flow = Flow[Message]
      .flatMapConcat {
        case TextMessage.Strict(str) =>
          System.out.println("------- Handler Flow strict text msg " + str)
          Source.single(str)
        case msg: TextMessage =>
          System.out.println("------- Handler Flow text msg")
          msg.textStream.fold("")(_ + _)
        case BinaryMessage.Strict(str) =>
          val msg = str.decodeString(StandardCharsets.UTF_8)
          System.out.println("------- Handler Flow binary strict " + msg)
          Source.single(msg)
        case msg: BinaryMessage =>
          System.out.println("------- Handler Flow binary")
          msg.dataStream.fold(ByteString.empty)(_ ++ _).map(_.decodeString(StandardCharsets.UTF_8))
        case unk =>
          System.out.println("!!!!!!!!!! Unexpected msg type " + unk)
          throw new IllegalStateException("Whoops")
      }
      .via(new WebSocketSessionManager(streamMeta, ks, flowComplete, register, subscribe))
      .flatMapMerge(Int.MaxValue, { s =>
        System.out.println("------- Handler Flow flat map: " + s)
        s
      })
      .map { obj =>
        System.out.println("------- Handler Flow resp " + obj)
        TextMessage(obj.toJson)
      }
      .via(ks.flow)
      .watchTermination() { (_, f) =>
        f.onComplete {
          // SO if we connect with a websock then kill it before sending a request, it won't unsubscribe.
          // Thus we can catch the disconnect here. We still need to see if the rest of the
          // stream is shut down.
          case Success(_) =>
            System.out.println(s"------- Handler Flow NORMAL disconnect? ${streamMeta.streamId}")
            sm.unregister(streamMeta.streamId)
            if (flowComplete.compareAndSet(false, true)) {
              System.out.println("               Completed gracefully.")
            }
          case Failure(t) =>
            System.out.println(
              s"------- Handler Flow ABNORMAL disconnect ${streamMeta.streamId} " + t.getMessage
            )
            t.printStackTrace()
            sm.unregister(streamMeta.streamId)
            if (flowComplete.compareAndSet(false, true)) {
              System.out.println("               Completed gracefully.")
            }
        }
      }
    System.out.println("### Finished handler flow")
    flow
  }

  /**
    * Uses a binary format for the messages and batches output to achieve higher throughput.
    */
  private def createHandlerFlowV2(streamMeta: StreamMetadata): Flow[Message, Message, Any] = {
    dropSameIdConnections(streamMeta)

    Flow[Message]
      .flatMapConcat {
        case msg: TextMessage =>
          // Text messages are not supported, ignore
          msg.textStream.runWith(Sink.ignore)
          Source.empty
        case BinaryMessage.Strict(str) =>
          Source.single(str)
        case msg: BinaryMessage =>
          msg.dataStream.fold(ByteString.empty)(_ ++ _)
      }
      .via(new WebSocketSessionManager(streamMeta, null, null, register, subscribe))
      .flatMapMerge(Int.MaxValue, msg => msg)
      .groupedWithin(batchSize, 1.second)
      .statefulMapConcat { () =>
        // Re-use the stream to reduce allocations
        val baos = new ByteArrayOutputStream()

        { seq =>
          List(BinaryMessage(LwcMessages.encodeBatch(seq, baos)))
        }
      }
  }

  private def stepAlignedTime(step: Long): Long = {
    registry.clock().wallTime() / step * step
  }

  private def register(
    streamMeta: StreamMetadata,
    ks: SharedKillSwitch,
    flowComplete: AtomicBoolean
  ): (QueueHandler, Source[JsonSupport, Unit]) = {
    System.out.println("### Registering stream " + streamMeta)
    val streamId = streamMeta.streamId

    // Create queue to allow messages coming into /evaluate to be passed to this stream
    System.out.println("### Registering stream creating queue. " + streamMeta)

    val (queue, pub) = StreamOps
      .blockingQueue[Seq[JsonSupport]](
        registry,
        "SubscribeApi",
        queueSize /*,
        materializer.executionContext*/
      )
      .watchTermination() { (q: SourceQueue[Seq[JsonSupport]], f) =>
        f.onComplete {
          case Success(_) =>
            System.out.println("********* QUEUE DONE!!!!")
            //throw new RuntimeException("Kablooey!")
            //sm.unregister(streamId)
            if (flowComplete.compareAndSet(false, true)) {
              System.out.println("*********** QUEUE Done and KS enganged")
              ks.shutdown()
            }
          case Failure(ex) =>
            System.out.println("********** QUEUE EX: " + ex.toString)
            //sm.unregister(streamId)
            if (flowComplete.compareAndSet(false, true)) {
              System.out.println("*********** QUEUE Done and KS enganged")
              ks.abort(ex)
            }
        }
        //Keep.left
        q
      }
      .toMat(Sink.asPublisher(true))(Keep.both)
      .run() /*(loggingMaterializer)*/

    System.out.println("### Registering finished creating queue. " + streamMeta)
    // Send initial setup messages
    queue.offer(Seq(DiagnosticMessage.info(s"setup stream $streamId on $instanceId")))
    val handler = new QueueHandler(streamMeta, queue)

    // Heartbeat messages to ensure that the socket is never idle
    val heartbeatSrc = Source
      .tick(0.seconds, 5.seconds, NotUsed)
      .flatMapConcat { _ =>
        System.out.println("------ HB flat map")
        val steps = sm
          .subscriptionsForStream(streamId)
          .map(_.metadata.frequency)
          .distinct
          .map { step =>
            System.out.println("------ Sending HB")
            // To account for some delays for data coming from real systems, the heartbeat
            // timestamp is delayed by one interval
            LwcHeartbeat(stepAlignedTime(step) - step, step)
          }
        Source(steps)
      }
      // USELESS
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) => System.out.println("********* HB DONE!!!!")
          case Failure(ex) =>
            System.out.println("********** HB EX: " + ex.toString)
        }
        Keep.right
      }
    System.out.println("### Registering setup HB source " + streamMeta)

    val source = Source
      .fromPublisher(pub)
      .log("PUB Maybe?")
      .named("FromQueuePub")
      .flatMapConcat(Source.apply)
      .merge(heartbeatSrc)
      .via(StreamOps.monitorFlow(registry, "StreamApi"))
      .watchTermination() { (_, f) =>
        f.onComplete {
          case Success(_) =>
            // calling the test probe subscriber.cancel() goes here.
            logger.debug(s"lost client for $streamId")
          //sm.unregister(streamId)
          case Failure(t) =>
            // calling the test probe subscriber.cancel(EX) goes here.
            logger.debug(s"lost client for $streamId", t)
          //sm.unregister(streamId)
        }
      }
    System.out.println("### Registered finished setting up flows " + streamMeta)

    sm.register(streamMeta, handler)
    System.out.println("### Registered " + streamMeta)

    handler -> source
  }

  private def subscribe(streamId: String, expressions: List[ExpressionMetadata]): List[ErrorMsg] = {
    evalsCounter.increment()
    itemsCounter.increment(expressions.size)

    val errors = scala.collection.mutable.ListBuffer[ErrorMsg]()
    val subIdsBuilder = Set.newBuilder[String]

    expressions.foreach { expr =>
      try {
        val splits = splitter.split(expr.expression, expr.frequency)

        // Add any new expressions
        val (queue, addedSubs) = sm.subscribe(streamId, splits)
        addedSubs.foreach { sub =>
          val meta = sub.metadata
          val exprInfo = LwcDataExpr(meta.id, meta.expression, meta.frequency)
          queue.offer(Seq(LwcSubscription(expr.expression, List(exprInfo))))
        }

        // Add expression ids in use by this split
        subIdsBuilder ++= splits.map(_.metadata.id)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Unable to subscribe to expression ${expr.expression}", e)
          errors += ErrorMsg(expr.expression, e.getMessage)
      }
    }

    // Remove any expressions that are no longer required
    val subIds = subIdsBuilder.result()
    sm.subscriptionsForStream(streamId)
      .filter(s => !subIds.contains(s.metadata.id))
      .foreach(s => sm.unsubscribe(streamId, s.metadata.id))

    errors.toList
  }
}

object SubscribeApi {

  private val instanceId = NetflixEnvironment.instanceId()

  case class SubscribeRequest(streamId: String, expressions: List[ExpressionMetadata])
      extends JsonSupport {

    require(streamId != null && streamId.nonEmpty, "streamId attribute is missing or empty")
    require(
      expressions != null && expressions.nonEmpty,
      "expressions attribute is missing or empty"
    )
  }

  case class ErrorMsg(expression: String, message: String)

  case class Errors(`type`: String, message: String, errors: List[ErrorMsg]) extends JsonSupport
}
