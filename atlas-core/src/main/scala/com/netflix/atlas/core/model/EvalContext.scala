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

case class EvalContext(
  start: Long,
  end: Long,
  step: Long,
  state: Map[StatefulExpr, Any] = IdentityMap.empty,
  steplessLimit: Option[Int] = None
) {

  require(start < end, s"start time must be less than end time ($start >= $end)")

  val noData: TimeSeries = TimeSeries.noData(step)

  var steplessStart: Long = 0
  var steplessEnd: Long = steplessLimit.getOrElse(0).toLong
  var steplessStep: Long = step

  /**
    * Buffer size that would be need to represent the result set based on the start time,
    * end time, and step size.
    */
  def bufferSize: Int = steplessLimit match {
    case Some(_) => ((steplessEnd - steplessStart) / steplessStep).toInt + 1
    case None    => ((end - start) / step).toInt + 1
  }

  def partition(oneStep: Long, unit: ChronoUnit): List[EvalContext] = {
    // TODO stepless
    val builder = List.newBuilder[EvalContext]
    var t = Instant.ofEpochMilli(start).truncatedTo(unit).toEpochMilli
    while (t < end) {
      val e = t + oneStep
      val stime = math.max(t, start)
      val etime = math.min(e, end)
      builder += EvalContext(stime, etime, step, steplessLimit = steplessLimit)
      t = e
    }
    builder.result()
  }

  def partitionByHour: List[EvalContext] =
    partition(Duration.ofHours(1).toMillis, ChronoUnit.HOURS)

  def partitionByDay: List[EvalContext] = partition(Duration.ofDays(1).toMillis, ChronoUnit.DAYS)

  def withOffset(offset: Long): EvalContext = {
    val dur = offset / step * step
    if (dur < step) this else EvalContext(start - dur, end - dur, step, state, steplessLimit)
  }

  def getStart: Long = steplessLimit.map(_ => steplessStart).getOrElse(start)
  def getEnd: Long = steplessLimit.map(_ => steplessEnd).getOrElse(end)
  def getStep: Long = steplessLimit.map(_ => steplessStep).getOrElse(step)
}
