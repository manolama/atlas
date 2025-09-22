package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter

trait EventQuery extends Expr {}

object EventQuery {

  case class EventTimeSeries(eventQuery: EventQuery, query: Query) extends Expr {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, eventQuery, query, Interpreter.WordToken(":event-time-series"))
    }

  }
}
