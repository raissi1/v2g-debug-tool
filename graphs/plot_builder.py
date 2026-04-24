"""Plotly graph builders for reconstructed session signals."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


SIGNALS = ["Ptarget", "P_meter", "P_dewesoft", "Qtarget", "Q_meter", "Q_dewesoft", "U_meter", "U_dewesoft", "frequency_meter", "frequency_dewesoft"]


def build_signal_figure(timeseries: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if timeseries.empty:
        fig.update_layout(title="Aucune donnée temporelle disponible")
        return fig

    for signal in SIGNALS:
        if signal in timeseries.columns and pd.to_numeric(timeseries[signal], errors="coerce").notna().any():
            fig.add_trace(
                go.Scatter(
                    x=timeseries["timestamp"],
                    y=timeseries[signal],
                    mode="lines",
                    name=signal,
                )
            )

    if "event_type" in timeseries.columns:
        events = timeseries[timeseries["event_type"].isin(["error", "timeout", "protocol_event", "gridcodes"])]
        for ts in events["timestamp"].dropna().head(50):
            fig.add_vline(x=ts, line_width=1, line_dash="dot", line_color="gray")

    fig.update_layout(title="Graphes comparatifs (target vs meter vs dewesoft)", xaxis_title="Temps", yaxis_title="Valeur")
    return fig
