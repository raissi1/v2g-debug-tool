"""Simple HTML report generation for V2G generic debug."""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def _to_list_html(values: list[str]) -> str:
    if not values:
        return "<li>Aucun</li>"
    return "".join(f"<li>{v}</li>" for v in values)


def _dewesoft_realtime_section(timeline: pd.DataFrame, detected_summary: dict | None) -> str:
    if timeline.empty:
        return "<p>Aucune donnée Dewesoft exploitable.</p>"

    work = timeline.copy()
    if "payload" not in work.columns:
        return "<p>Aucune donnée Dewesoft exploitable.</p>"

    src_group = work["payload"].apply(lambda p: p.get("source_group") if isinstance(p, dict) else None)
    dew = work[src_group.astype(str).str.contains("measure", case=False, na=False)].copy()
    if dew.empty:
        return "<p>Aucune donnée Dewesoft exploitable.</p>"

    dew["timestamp"] = pd.to_datetime(dew["timestamp"], utc=True, errors="coerce")
    stats_lines: list[str] = []
    for label, col in [
        ("P", "P"),
        ("Q", "Q"),
        ("U", "U"),
        ("fréquence", "frequency"),
    ]:
        series = pd.to_numeric(dew[col], errors="coerce") if col in dew.columns else pd.Series(dtype=float)
        if series.dropna().empty:
            stats_lines.append(f"{label}: non disponible")
        else:
            stats_lines.append(f"{label}: min={series.min():.3f}, max={series.max():.3f}, moyenne={series.mean():.3f}")

    anomalies: list[str] = []
    p = pd.to_numeric(dew["P"], errors="coerce") if "P" in dew.columns else pd.Series(dtype=float)
    if not p.dropna().empty and (p.abs() < 0.1).mean() > 0.8:
        anomalies.append("Puissance Dewesoft quasi nulle sur la majorité de la période.")

    meter = work[src_group.astype(str).str.contains("meter_dispatcher", case=False, na=False)].copy()
    compare_line = "Comparaison meter interne impossible (données manquantes)."
    if not meter.empty and "P" in meter.columns and "P" in dew.columns:
        meter_p = pd.to_numeric(meter["P"], errors="coerce").dropna()
        dew_p = pd.to_numeric(dew["P"], errors="coerce").dropna()
        if not meter_p.empty and not dew_p.empty:
            compare_line = f"Comparaison P meter vs Dewesoft (moyennes): {meter_p.mean():.3f} vs {dew_p.mean():.3f}."

    files_lines = []
    if detected_summary:
        files_lines.append(f"CSV détectés: {len(detected_summary.get('dewesoft_csv', []))}")
        files_lines.append(f"Brut d7d/dxd: {len(detected_summary.get('dewesoft_raw', []))}")
    period = "Période couverte: inconnue"
    ts = dew["timestamp"].dropna()
    if not ts.empty:
        period = f"Période couverte: {ts.min().isoformat()} → {ts.max().isoformat()}"

    return (
        f"<ul>{_to_list_html(files_lines + [period] + stats_lines + [compare_line] + anomalies)}</ul>"
    )


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
            f"PCAP total: {len(detected_summary.get('netlogger_pcaps', [])) + len(detected_summary.get('generic_pcaps', []))}",
            f"PCAP netlogger: {len(detected_summary.get('netlogger_pcaps', []))}",
            f"PCAP pcap/pcaps: {len(detected_summary.get('generic_pcaps', []))}",
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
    cross = diagnostic.get("cross_analysis", {})
    cross_insights_html = _to_list_html(cross.get("insights", []))
    cross_rows = pd.DataFrame(cross.get("rows", []))
    cross_table_html = "<p>Aucune table comparative disponible.</p>"
    if not cross_rows.empty:
        keep = [c for c in ["timestamp", "Ptarget", "Qtarget", "P_meter", "Q_meter", "P_dewesoft", "Q_dewesoft", "U_meter", "U_dewesoft", "frequency_meter", "frequency_dewesoft", "event_type", "message"] if c in cross_rows.columns]
        cross_table_html = cross_rows[keep].head(200).to_html(index=False, escape=True)
    dewesoft_section_html = _dewesoft_realtime_section(timeline, detected_summary)
    evidence_rows = pd.DataFrame(diagnostic.get("evidence_table", []))
    evidence_table_html = "<p>Aucune preuve structurée.</p>"
    if not evidence_rows.empty:
        keep = [c for c in ["timestamp", "source", "type", "extracted_value", "impact", "weight", "comment"] if c in evidence_rows.columns]
        evidence_table_html = evidence_rows[keep].to_html(index=False, escape=True)

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

        <h2>Analyse croisée des sources</h2>
        <h3>A. Ce que la borne demande/calcul</h3>
        <ul>{_to_list_html(blocks.get("A_requested", []))}</ul>
        <h3>B. Ce que le protocole envoie</h3>
        <ul>{_to_list_html(blocks.get("C_sent_to_vehicle", []))}</ul>
        <h3>C. Ce que le meter interne mesure</h3>
        <ul>{_to_list_html(blocks.get("D_measured", []))}</ul>
        <h3>D. Ce que Dewesoft mesure</h3>
        <ul>{_to_list_html([line for line in blocks.get("D_measured", []) if "dewesoft" in line.lower()])}</ul>
        <h3>E. Écarts détectés</h3>
        <ul>{cross_insights_html}</ul>
        <h3>F. Conclusion probable</h3>
        <p><strong>{diagnostic.get('conclusion', 'Indéterminé')}</strong> (Confiance: {diagnostic.get('confidence', 'Faible')})</p>
        {cross_table_html}

        <h2>Analyse Dewesoft temps réel</h2>
        {dewesoft_section_html}

        <h2>Preuves du diagnostic</h2>
        {evidence_table_html}

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
