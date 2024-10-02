/*
 * Copyright 2014-2024 Netflix, Inc.
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
package com.netflix.atlas.core.model

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import com.netflix.atlas.core.util.IdentityMap

import scala.util.Failure
import scala.util.Try

class EvalContext(
  val queryStart: Long,
  val queryEnd: Long,
  val step: Long,
  val state: Map[StatefulExpr, Any] = IdentityMap.empty,
  val steplessLimit: Option[Long] = None
) {

  require(
    queryStart < queryEnd,
    s"start time must be less than end time ($queryStart >= $queryEnd)"
  )

  val noData: TimeSeries = TimeSeries.noData(step)

  private var computedStart: Option[Long] =
    None // = steplessLimit.map(_ => 0L).getOrElse(queryStart)
  private var computedEnd: Option[Long] = None // = steplessLimit.getOrElse(queryEnd)

  /**
    * Buffer size that would be need to represent the result set based on the start time,
    * end time, and step size.
    */
  def bufferSize: Int = ((start - end) / step).toInt + 1

  def partition(oneStep: Long, unit: ChronoUnit): List[EvalContext] = {
    // TODO stepless
    val builder = List.newBuilder[EvalContext]
    var t = Instant.ofEpochMilli(start).truncatedTo(unit).toEpochMilli
    while (t < end) {
      val e = t + oneStep
      val stime = math.max(t, start)
      val etime = math.min(e, end)
      builder += new EvalContext(stime, etime, step, steplessLimit = steplessLimit)
      t = e
    }
    builder.result()
  }

  def partitionByHour: List[EvalContext] =
    partition(Duration.ofHours(1).toMillis, ChronoUnit.HOURS)

  def partitionByDay: List[EvalContext] = partition(Duration.ofDays(1).toMillis, ChronoUnit.DAYS)

  def withOffset(offset: Long): EvalContext = {
    val dur = offset / step * step
    if (dur < step) this
    else new EvalContext(queryStart - dur, queryEnd - dur, step, state, steplessLimit)
  }

  def start: Long = computedStart.getOrElse(steplessLimit.map(_ => 0L).getOrElse(queryStart))
  def end: Long = computedEnd.getOrElse(steplessLimit.getOrElse(queryEnd))

  def update(start: Long, end: Long): Unit = {
    if (computedStart.isDefined) {
      computedStart = Some(math.max(start, computedStart.get))
    } else {
      computedStart = Some(start)
    }
    if (computedEnd.isDefined) {
      computedEnd = Some(math.max(end, computedEnd.get))
    } else {
      computedEnd = Some(end)
    }
  }

  def update(context: EvalContext): Unit = {
    (computedStart, context.computedStart) match {
      case (Some(cs), Some(ocs)) => computedStart = Some(math.max(cs, ocs))
      case (None, Some(ocs))     => computedStart = Some(ocs)
      case _                     =>
    }
    (computedEnd, context.computedEnd) match {
      case (Some(ce), Some(oce)) => computedEnd = Some(math.max(ce, oce))
      case (None, Some(oce))     => computedEnd = Some(oce)
      case _                     =>
    }
  }

  def cloneWithState(state: Map[StatefulExpr, Any]): EvalContext = {
    val ctx = new EvalContext(queryStart, queryEnd, step, state, steplessLimit)
    ctx.computedStart = computedStart
    ctx.computedEnd = computedEnd
    ctx
  }
}
