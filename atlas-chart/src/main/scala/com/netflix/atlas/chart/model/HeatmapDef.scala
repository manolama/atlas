package com.netflix.atlas.chart.model

import com.netflix.atlas.chart.model.PlotBound.AutoStyle

case class HeatmapDef(
  scale: Scale = Scale.LINEAR,
  upper: PlotBound = AutoStyle,
  lower: PlotBound = AutoStyle,
  palette: Option[Palette] = None
)
