/*
 * Copyright 2014-2025 Netflix, Inc.
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

import com.netflix.atlas.core.model.EventExpr.Raw
import com.netflix.atlas.core.model.TraceQuery.SpanTimeSeries
import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word

object EventVocabulary extends Vocabulary {

  val name: String = "event"

  val dependsOn: List[Vocabulary] = List(QueryVocabulary)

  override def words: List[Word] = List(SampleWord, TableWord, EventTimeSeriesWord)

  case object TableWord extends SimpleWord {

    import ModelExtractors.*

    override def name: String = "table"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case StringListType(_) :: (_: Query) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case StringListType(cs) :: (q: Query) :: stack => EventExpr.Table(q, cs) :: stack
    }

    override def signature: String = "q:Query columns:List -- EventExpr"

    override def summary: String =
      """
        |Find matching events and create a row by extracting the specified columns.
        |""".stripMargin

    override def examples: List[String] = List("level,ERROR,:eq,(,message,)")
  }

  case object SampleWord extends SimpleWord {

    import ModelExtractors.*

    override def name: String = "sample"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case StringListType(_) :: StringListType(_) :: (_: Query) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case StringListType(pks) :: StringListType(by) :: (q: Query) :: stack =>
        EventExpr.Sample(q, by, pks) :: stack
    }

    override def signature: String = "q:Query sampleBy:List projectionKeys:List -- EventExpr"

    override def summary: String =
      """
        |Find matching events and sample based on a set of keys. The output will be a count
        |for the step interval along with some sample data for that group based on the projection
        |keys.
        |""".stripMargin

    override def examples: List[String] = List("level,ERROR,:eq,(,fingerprint,),(,message,)")
  }

  case object EventTimeSeriesWord extends SimpleWord {

    override def name: String = "event-time-series"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: (_: SpanTimeSeries) :: _ => false
      case (_: Query) :: (_: EventQuery) :: _     => true
      case (_: SpanTimeSeries) :: _               => false
      case (_: EventQuery) :: _                   => true
      case (_: Query) :: _                        => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (q: Query) :: (e: EventExpr.Sample) :: stack =>
        // the value has to be `event.count` as aggregating on a field from a sample per
        // period is meaningless
        if (q != Query.Equal("value", "event.count"))
          throw new IllegalArgumentException(
            s"when using :event-time-series with :sample, query has to be value,event.count"
          )
        EventQuery.EventTimeSeries(e, q) :: stack

      case (q: Query) :: (e: EventQuery) :: stack =>
        EventQuery.EventTimeSeries(e, q) :: stack

      case (e: EventQuery) :: stack =>
        // default to count
        EventQuery.EventTimeSeries(e, Query.Equal("value", "event.count")) :: stack

      case (q: Query) :: stack =>
        // default to count
        EventQuery.EventTimeSeries(Raw(q), Query.Equal("value", "event.count")) :: stack
    }

    override def signature: String = "EventQuery Query -- EventTimeSeries"

    override def summary: String =
      """
            |TODO.
            |""".stripMargin

    override def examples: List[String] = List(
      "level,ERROR,:eq,(,fingerprint,),(,message,),:sample,value,event.count,:eq"
    )
  }
}
