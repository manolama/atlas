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

class EventExamplesSuite extends BaseExamplesSuite {

  override def vocabulary: Vocabulary = EventVocabulary

  test("event-time-series - table default count") {
    interpreter.execute("level,ERROR,:eq,(,message,),:table,:event-time-series")
  }

  test("event-time-series - table explicit column column") {
    interpreter.execute("level,ERROR,:eq,(,message,),:table,value,foo,:eq,:event-time-series")
  }

  test("event-time-series - just a query") {
    interpreter.execute("level,ERROR,:eq,:event-time-series")
  }

  test("event-time-series - sample direct") {
    interpreter.execute("level,ERROR,:eq,(,fingerprint,),(,message,),:sample,:event-time-series")
  }

  test("event-time-series - sample with explicit value") {
    interpreter.execute(
      "level,ERROR,:eq,(,fingerprint,),(,message,),:sample,value,event.count,:eq,:event-time-series"
    )
  }

  test("event-time-series - sample with value missmatch") {
    intercept[IllegalArgumentException] {
      interpreter.execute(
        "level,ERROR,:eq,(,fingerprint,),(,message,),:sample,value,foo,:eq,:event-time-series"
      )
    }
  }
}
