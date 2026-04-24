"""Simple HTML report generation for V2G generic debug."""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def _to_list_html(values: list[str]) -> str:
    if not values:
        return "<li>Aucun</li>"
    return "".join(f"<li>{v}</li>" for v in values)


def generate_html_report(
    summary_lines: list[str],
    diagnostic: dict,
    timeline: pd.DataFrame,
    detected_summary: dict | None = None,
) -> str:
    issues_html = _to_list_html(diagnostic.get("issues", []))
    summary_html = _to_list_html(summary_lines)

    files_html = "<li>Non fourni</li>"
    if detected_summary:
        compact = [
            f"EnergyManager: {len(detected_summary.get('energy_manager', []))}",
            f"ChargerApp: {len(detected_summary.get('charger_app', []))}",
            f"Meter dispatcher: {len(detected_summary.get('iotc_meter_dispatcher', []))}",
            f"PCAP netlogger: {len(detected_summary.get('netlogger_pcaps', []))}",
            f"Mesures Dewesoft CSV: {len(detected_summary.get('dewesoft_csv', []))}",
        ]
        files_html = _to_list_html(compact)

    blocks = diagnostic.get("blocks", {})
    blocks_html = "".join(
        f"<h3>{title}</h3><ul>{_to_list_html(lines)}</ul>"
        for title, lines in [
            ("A. Ce qui a été demandé", blocks.get("A_requested", [])),
            ("B. Ce que la borne a calculé/publié", blocks.get("B_station_computed", [])),
            ("C. Ce qui a été envoyé au véhicule", blocks.get("C_sent_to_vehicle", [])),
            ("D. Ce qui a été mesuré", blocks.get("D_measured", [])),
            ("E. Anomalies détectées", blocks.get("E_anomalies", [])),
        ]
    )

    table_html = "<p>Aucune donnée timeline.</p>"
    if not timeline.empty:
        preview = timeline.head(300)
        table_html = preview[[c for c in ["timestamp", "source", "event_type", "message", "interpretation", "extracted_value"] if c in preview.columns]].to_html(index=False, escape=True)

    evidence_html = _to_list_html(diagnostic.get("evidence", []))
    missing_html = _to_list_html(diagnostic.get("missing_data", []))

    return f"""
    <html>
      <head><meta charset='utf-8'><title>V2G Debug Report</title></head>
      <body>
        <h1>Rapport de debug V2G</h1>
        <p>Généré le: {datetime.utcnow().isoformat()}Z</p>

        <h2>Résumé exécutif</h2>
        <p>{diagnostic.get('executive_summary', '')}</p>

        <h2>Résumé détaillé</h2>
        <ul>{summary_html}</ul>

        <h2>Fichiers détectés</h2>
        <ul>{files_html}</ul>

        <h2>Raisonnement du diagnostic</h2>
        {blocks_html}

        <h2>Anomalies détectées</h2>
        <ul>{issues_html}</ul>

        <h2>Conclusion</h2>
        <p><strong>{diagnostic.get('conclusion', 'Indéterminé')}</strong> (Confiance: {diagnostic.get('confidence', 'Faible')})</p>

        <h2>Preuves utilisées</h2>
        <ul>{evidence_html}</ul>

        <h2>Données manquantes</h2>
        <ul>{missing_html}</ul>

        <h2>Timeline (aperçu)</h2>
        {table_html}
      </body>
    </html>
    """
