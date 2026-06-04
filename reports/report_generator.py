"""Simple HTML report generation for V2G generic debug."""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape

import pandas as pd


CAUSE_LABELS = {
    "borne": "Cote borne",
    "vehicule": "Cote vehicule",
    "communication": "Cote communication",
    "indetermine": "Indetermine",
}


def _label_cause(value: str | None) -> str:
    return CAUSE_LABELS.get(str(value or "").lower(), str(value or "Indetermine"))


def _to_list_html(values: list[str]) -> str:
    if not values:
        return "<li>Aucun element exploitable</li>"
    return "".join(f"<li>{escape(str(v))}</li>" for v in values)


def _to_table_html(frame: pd.DataFrame, columns: list[str], fallback: str, limit: int) -> str:
    if frame.empty:
        return f"<p>{escape(fallback)}</p>"
    keep = [column for column in columns if column in frame.columns]
    if not keep:
        return f"<p>{escape(fallback)}</p>"
    return frame[keep].head(limit).to_html(index=False, escape=True)


def _build_file_summary(detected_summary: dict | None) -> list[str]:
    if not detected_summary:
        return ["Detection des fichiers non fournie."]

    pcap_total = len(detected_summary.get("netlogger_pcaps", [])) + len(detected_summary.get("generic_pcaps", []))
    return [
        f"EnergyManager: {len(detected_summary.get('energy_manager', []))} fichier(s)",
        f"ChargerApp: {len(detected_summary.get('charger_app', []))} fichier(s)",
        f"Meter dispatcher: {len(detected_summary.get('iotc_meter_dispatcher', []))} fichier(s)",
        f"Logs generiques: {len(detected_summary.get('generic_logs', []))} fichier(s)",
        f"PCAP detectes: {pcap_total} fichier(s)",
        f"PCAP netlogger: {len(detected_summary.get('netlogger_pcaps', []))} fichier(s)",
        f"PCAP dossiers pcap/pcaps: {len(detected_summary.get('generic_pcaps', []))} fichier(s)",
        f"Mesures Dewesoft CSV: {len(detected_summary.get('dewesoft_csv', []))} fichier(s)",
        f"Mesures Dewesoft brutes (.d7d/.dxd): {len(detected_summary.get('dewesoft_raw', []))} fichier(s)",
    ]


def _dewesoft_realtime_section(timeline: pd.DataFrame, detected_summary: dict | None) -> str:
    if timeline.empty or "payload" not in timeline.columns:
        return "<p>Aucune donnee Dewesoft exploitable.</p>"

    work = timeline.copy()
    src_group = work["payload"].apply(lambda payload: payload.get("source_group") if isinstance(payload, dict) else None)
    dew = work[src_group.astype(str).str.contains("measure", case=False, na=False)].copy()
    if dew.empty:
        return "<p>Aucune donnee Dewesoft exploitable.</p>"

    dew["timestamp"] = pd.to_datetime(dew["timestamp"], utc=True, errors="coerce")
    stats_lines: list[str] = []
    for label, column in [("P", "P"), ("Q", "Q"), ("U", "U"), ("Frequence", "frequency")]:
        series = pd.to_numeric(dew[column], errors="coerce") if column in dew.columns else pd.Series(dtype=float)
        if series.dropna().empty:
            stats_lines.append(f"{label}: non disponible")
        else:
            stats_lines.append(
                f"{label}: min={series.min():.3f}, max={series.max():.3f}, moyenne={series.mean():.3f}"
            )

    anomalies: list[str] = []
    p_series = pd.to_numeric(dew["P"], errors="coerce") if "P" in dew.columns else pd.Series(dtype=float)
    if not p_series.dropna().empty and (p_series.abs() < 0.1).mean() > 0.8:
        anomalies.append("Puissance Dewesoft quasi nulle sur la majorite de la periode.")

    meter = work[src_group.astype(str).str.contains("meter_dispatcher", case=False, na=False)].copy()
    comparison_line = "Comparaison meter interne vs Dewesoft impossible (donnees manquantes)."
    if not meter.empty and "P" in meter.columns and "P" in dew.columns:
        meter_p = pd.to_numeric(meter["P"], errors="coerce").dropna()
        dew_p = pd.to_numeric(dew["P"], errors="coerce").dropna()
        if not meter_p.empty and not dew_p.empty:
            comparison_line = (
                "Comparaison P meter interne vs Dewesoft "
                f"(moyennes): {meter_p.mean():.3f} vs {dew_p.mean():.3f}."
            )

    files_lines = []
    if detected_summary:
        files_lines.append(f"CSV Dewesoft detectes: {len(detected_summary.get('dewesoft_csv', []))}")
        files_lines.append(f"Fichiers bruts Dewesoft (.d7d/.dxd): {len(detected_summary.get('dewesoft_raw', []))}")

    coverage = "Periode couverte: inconnue"
    timestamps = dew["timestamp"].dropna()
    if not timestamps.empty:
        coverage = f"Periode couverte: {timestamps.min().isoformat()} -> {timestamps.max().isoformat()}"

    return f"<ul>{_to_list_html(files_lines + [coverage] + stats_lines + [comparison_line] + anomalies)}</ul>"


def generate_html_report(
    summary_lines: list[str],
    diagnostic: dict,
    timeline: pd.DataFrame,
    detected_summary: dict | None = None,
) -> str:
    blocks = diagnostic.get("blocks", {})
    cross = diagnostic.get("cross_analysis", {})
    cross_rows = pd.DataFrame(cross.get("rows", []))
    evidence_rows = pd.DataFrame(diagnostic.get("evidence_table", []))

    cause = diagnostic.get("cause_probable", "indetermine")
    cause_label = _label_cause(cause)
    confidence = diagnostic.get("confidence", "Faible")
    confidence_score = diagnostic.get("confidence_score", 0)

    requested_lines = blocks.get("A_requested", [])
    station_lines = blocks.get("B_station_computed", [])
    protocol_lines = blocks.get("C_sent_to_vehicle", [])
    measured_lines = blocks.get("D_measured", [])
    anomaly_lines = blocks.get("E_anomalies", [])

    protocol_insights = cross.get("insights", [])
    dewesoft_lines = [line for line in measured_lines if "dewesoft" in line.lower()]

    timeline_html = _to_table_html(
        timeline,
        ["timestamp", "source", "event_type", "message", "interpretation", "extracted_value"],
        "Aucune donnee timeline exploitable.",
        limit=300,
    )
    cross_table_html = _to_table_html(
        cross_rows,
        [
            "timestamp",
            "Ptarget",
            "Qtarget",
            "P_meter",
            "Q_meter",
            "P_dewesoft",
            "Q_dewesoft",
            "U_meter",
            "U_dewesoft",
            "frequency_meter",
            "frequency_dewesoft",
            "event_type",
            "message",
        ],
        "Aucune table comparative disponible.",
        limit=200,
    )
    evidence_table_html = _to_table_html(
        evidence_rows,
        ["timestamp", "source", "type", "extracted_value", "impact", "weight", "comment"],
        "Aucune preuve structuree.",
        limit=300,
    )

    return f"""
    <html>
      <head>
        <meta charset="utf-8">
        <title>V2G Debug Report</title>
        <style>
          body {{
            font-family: "Segoe UI", Arial, sans-serif;
            margin: 24px;
            color: #1f2937;
            background: #f6f7fb;
          }}
          h1, h2, h3 {{
            color: #0f172a;
          }}
          .hero {{
            background: linear-gradient(135deg, #0f766e, #164e63);
            color: white;
            padding: 24px;
            border-radius: 18px;
            margin-bottom: 20px;
          }}
          .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 12px;
            margin: 20px 0;
          }}
          .card {{
            background: white;
            border-radius: 14px;
            padding: 16px;
            box-shadow: 0 6px 20px rgba(15, 23, 42, 0.08);
          }}
          .section {{
            background: white;
            border-radius: 14px;
            padding: 18px;
            margin-top: 16px;
            box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
          }}
          table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
          }}
          th, td {{
            border: 1px solid #dbe1ea;
            padding: 8px;
            vertical-align: top;
          }}
          th {{
            background: #eaf2ff;
          }}
          ul {{
            margin-top: 8px;
          }}
          .verdict {{
            font-size: 28px;
            font-weight: 700;
            margin: 8px 0 0 0;
          }}
          .muted {{
            color: #475569;
          }}
        </style>
      </head>
      <body>
        <div class="hero">
          <h1>Rapport de debug V2G</h1>
          <p>Genere le {datetime.now(timezone.utc).isoformat()}</p>
          <div class="verdict">{escape(cause_label)}</div>
          <p>Confiance: {escape(str(confidence))} ({escape(str(confidence_score))}%)</p>
          <p>{escape(diagnostic.get("executive_summary", ""))}</p>
        </div>

        <div class="grid">
          <div class="card">
            <h3>Verdict</h3>
            <p><strong>{escape(cause_label)}</strong></p>
            <p class="muted">{escape(diagnostic.get("justification", ""))}</p>
          </div>
          <div class="card">
            <h3>Donnees manquantes</h3>
            <ul>{_to_list_html(diagnostic.get("missing_data", []))}</ul>
          </div>
          <div class="card">
            <h3>Preuves fortes</h3>
            <ul>{_to_list_html(diagnostic.get("evidence", []))}</ul>
          </div>
        </div>

        <div class="section">
          <h2>Entrees detectees</h2>
          <ul>{_to_list_html(_build_file_summary(detected_summary))}</ul>
        </div>

        <div class="section">
          <h2>Resume de session</h2>
          <ul>{_to_list_html(summary_lines)}</ul>
        </div>

        <div class="section">
          <h2>Raisonnement metier</h2>
          <h3>1. Ce qui a ete demande</h3>
          <ul>{_to_list_html(requested_lines)}</ul>
          <h3>2. Ce que la borne a calcule ou publie</h3>
          <ul>{_to_list_html(station_lines)}</ul>
          <h3>3. Ce qui a ete envoye au vehicule</h3>
          <ul>{_to_list_html(protocol_lines)}</ul>
          <h3>4. Ce qui a ete mesure</h3>
          <ul>{_to_list_html(measured_lines)}</ul>
          <h3>5. Anomalies retenues</h3>
          <ul>{_to_list_html(anomaly_lines)}</ul>
        </div>

        <div class="section">
          <h2>Correlation inter-sources</h2>
          <p class="muted">
            Cette section rassemble les consignes, la reponse de la borne, les echanges protocole,
            les mesures du meter interne et les mesures Dewesoft pour localiser l'ecart principal.
          </p>
          <h3>Ecarts detectes</h3>
          <ul>{_to_list_html(protocol_insights)}</ul>
          <h3>Focus Dewesoft</h3>
          <ul>{_to_list_html(dewesoft_lines)}</ul>
          {cross_table_html}
        </div>

        <div class="section">
          <h2>Analyse Dewesoft temps reel</h2>
          {_dewesoft_realtime_section(timeline, detected_summary)}
        </div>

        <div class="section">
          <h2>Preuves structurees</h2>
          {evidence_table_html}
        </div>

        <div class="section">
          <h2>Timeline de reference</h2>
          {timeline_html}
        </div>
      </body>
    </html>
    """
