"""Plotly graph builders for reconstructed session signals."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


SIGNALS = ["P", "Q", "U", "frequency", "Ptarget", "Qtarget"]


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

    fig.update_layout(title="Graphes P/Q/U/fréquence et consignes", xaxis_title="Temps", yaxis_title="Valeur")
    return fig
