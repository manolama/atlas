package com.netflix.atlas.chart.graphics

trait XAxis extends Element with FixedHeight {

  def ticks(x1: Int, x2: Int): List[TimeTick]

  def scale(p1: Int, p2: Int): Scales.LongScale

  def start: Long

  def end: Long

  def step: Long

  def size: Int
}
