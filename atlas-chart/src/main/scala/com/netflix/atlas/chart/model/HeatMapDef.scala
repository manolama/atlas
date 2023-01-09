package com.netflix.atlas.chart.model

import com.netflix.atlas.chart.model.PlotBound.AutoStyle

/**
  * A configuration used to compute and optionally plot a heatmap.
  *
  * @param colorScale
  *   The color scale to use for counts within a cell.
  * @param upper
  *   An optional upper boundary for the cell count.
  * @param lower
  *   An optional lower boundary for the cell count.
  * @param palette
  *   An optional palette to use for the heatmap
  * @param legend
  *   A string to use for the legend.
  */
case class HeatMapDef(
  colorScale: Scale = Scale.LINEAR,
  upper: PlotBound = AutoStyle,
  lower: PlotBound = AutoStyle,
  palette: Option[Palette] = None,
  legend: Option[String] = None
)
