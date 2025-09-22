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

import com.netflix.atlas.core.stacklang.BaseExamplesSuite
import com.netflix.atlas.core.stacklang.Vocabulary

class TraceExamplesSuite extends BaseExamplesSuite {

  override def vocabulary: Vocabulary = TraceVocabulary

  test("event-time-series - child default count") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:child,:event-time-series")
  }

  test("event-time-series - child explicit value") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:child,value,baz,:eq,:event-time-series")
  }

  test("event-time-series - span-and default count") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:span-and,:event-time-series")
  }

  test("event-time-series - span-and explicit value") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:span-and,value,baz,:eq,:event-time-series")
  }

  test("event-time-series - span-or default count") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:span-or,:event-time-series")
  }

  test("event-time-series - span-or explicit value") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:span-or,value,baz,:eq,:event-time-series")
  }

  test("event-time-series - span-filter default count") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:span-filter,:event-time-series")
  }

  test("event-time-series - span-filter explicit value") {
    interpreter.execute("app,foo,:eq,app,bar,:eq,:span-filter,value,baz,:eq,:event-time-series")
  }

  test("event-time-series - fail on span-time-series, default count") {
    intercept[IllegalStateException] {
      interpreter.execute(
        "app,foo,:eq,app,bar,:eq,:span-filter,value,count,:span-time-series,:event-time-series"
      )
    }
  }

  test("event-time-series - fail on span-time-series, explicit value") {
    intercept[IllegalStateException] {
      interpreter.execute(
        "app,foo,:eq,app,bar,:eq,:span-filter,value,count,:span-time-series,value,baz,:eq,:event-time-series"
      )
    }
  }
}
