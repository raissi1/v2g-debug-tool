"""Styled HTML report generation for the V2G debug workflow."""

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


def _to_list_html(values: list[str], empty_label: str = "Aucun element exploitable") -> str:
    if not values:
        return f"<li>{escape(empty_label)}</li>"
    return "".join(f"<li>{escape(str(value))}</li>" for value in values)


def _to_table_html(frame: pd.DataFrame, columns: list[str], fallback: str, limit: int) -> str:
    if frame.empty:
        return f"<p class=\"empty\">{escape(fallback)}</p>"
    keep = [column for column in columns if column in frame.columns]
    if not keep:
        return f"<p class=\"empty\">{escape(fallback)}</p>"
    return frame[keep].head(limit).to_html(index=False, escape=True, classes="report-table")


def _metric_card(label: str, value: str, tone: str = "neutral") -> str:
    return (
        f"<div class=\"metric-card tone-{escape(tone)}\">"
        f"<div class=\"metric-label\">{escape(label)}</div>"
        f"<div class=\"metric-value\">{escape(value)}</div>"
        "</div>"
    )


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


def _source_matrix_html(detected_summary: dict | None) -> str:
    if not detected_summary:
        return "<p class=\"empty\">Detection des sources indisponible.</p>"

    rows = [
        {
            "Source": "EnergyManager",
            "Role": "Consignes, recalculs, limitations, erreurs applicatives",
            "Count": len(detected_summary.get("energy_manager", [])),
        },
        {
            "Source": "ChargerApp",
            "Role": "Echanges cote borne / vehicule, contexte de session",
            "Count": len(detected_summary.get("charger_app", [])),
        },
        {
            "Source": "Meter dispatcher",
            "Role": "Mesures internes borne",
            "Count": len(detected_summary.get("iotc_meter_dispatcher", [])),
        },
        {
            "Source": "PCAP",
            "Role": "Preuves protocole et transitions de consigne",
            "Count": len(detected_summary.get("netlogger_pcaps", [])) + len(detected_summary.get("generic_pcaps", [])),
        },
        {
            "Source": "Dewesoft CSV",
            "Role": "Mesures physiques de reference",
            "Count": len(detected_summary.get("dewesoft_csv", [])),
        },
    ]
    frame = pd.DataFrame(rows)
    return frame.to_html(index=False, escape=True, classes="report-table")


def _build_recommendations(diagnostic: dict, detected_summary: dict | None) -> list[str]:
    recommendations: list[str] = []
    cause = str(diagnostic.get("cause_probable", "indetermine")).lower()
    missing_data = [str(item) for item in diagnostic.get("missing_data", [])]

    if cause == "borne":
        recommendations.append("Verifier la logique de limitation ou de recalcul cote borne autour des transitions de consigne.")
    elif cause == "vehicule":
        recommendations.append("Comparer la consigne envoyee et la reponse physique du vehicule sur la meme fenetre temporelle.")
    elif cause == "communication":
        recommendations.append("Rejouer la sequence protocole pour confirmer les timeouts, absences de reponse ou erreurs de transport.")
    else:
        recommendations.append("Recouper davantage les consignes, les mesures internes et les acquisitions externes avant de conclure.")

    if "Ptarget" in missing_data:
        recommendations.append("Extraire une trace plus explicite des consignes Ptarget depuis EnergyManager ou PCAP.")
    if detected_summary and detected_summary.get("dewesoft_raw", []) and not detected_summary.get("dewesoft_csv", []):
        recommendations.append("Convertir les fichiers Dewesoft bruts (.d7d/.dxd) en CSV pour exploiter les mesures detaillees.")
    elif detected_summary and not detected_summary.get("dewesoft_csv", []):
        recommendations.append("Ajouter un export Dewesoft CSV pour renforcer le diagnostic physique.")
    if detected_summary and not (detected_summary.get("netlogger_pcaps", []) or detected_summary.get("generic_pcaps", [])):
        recommendations.append("Ajouter une capture PCAP exploitable pour mieux justifier la partie protocole.")

    return recommendations


def _dewesoft_realtime_section(timeline: pd.DataFrame, detected_summary: dict | None) -> str:
    if timeline.empty or "payload" not in timeline.columns:
        if detected_summary and detected_summary.get("dewesoft_raw", []):
            return "<p class=\"empty\">Dewesoft brut detecte, mais aucune mesure exploitable sans conversion CSV.</p>"
        return "<p class=\"empty\">Aucune donnee Dewesoft exploitable.</p>"

    work = timeline.copy()
    src_group = work["payload"].apply(lambda payload: payload.get("source_group") if isinstance(payload, dict) else None)
    dew = work[src_group.astype(str).str.contains("measure", case=False, na=False)].copy()
    if dew.empty:
        if detected_summary and detected_summary.get("dewesoft_raw", []):
            return "<p class=\"empty\">Dewesoft brut detecte, mais aucune mesure exploitable sans conversion CSV.</p>"
        return "<p class=\"empty\">Aucune donnee Dewesoft exploitable.</p>"

    dew["timestamp"] = pd.to_datetime(dew["timestamp"], utc=True, errors="coerce")
    stats_lines: list[str] = []
    for label, column in [("P", "P"), ("Q", "Q"), ("U", "U"), ("Frequence", "frequency")]:
        series = pd.to_numeric(dew[column], errors="coerce") if column in dew.columns else pd.Series(dtype=float)
        if series.dropna().empty:
            stats_lines.append(f"{label}: non disponible")
        else:
            stats_lines.append(f"{label}: min={series.min():.3f}, max={series.max():.3f}, moyenne={series.mean():.3f}")

    p_series = pd.to_numeric(dew["P"], errors="coerce") if "P" in dew.columns else pd.Series(dtype=float)
    if not p_series.dropna().empty and (p_series.abs() < 0.1).mean() > 0.8:
        stats_lines.append("Observation: puissance Dewesoft quasi nulle sur la majorite de la periode.")

    meter = work[src_group.astype(str).str.contains("meter_dispatcher", case=False, na=False)].copy()
    if not meter.empty and "P" in meter.columns and "P" in dew.columns:
        meter_p = pd.to_numeric(meter["P"], errors="coerce").dropna()
        dew_p = pd.to_numeric(dew["P"], errors="coerce").dropna()
        if not meter_p.empty and not dew_p.empty:
            stats_lines.append(f"Comparaison P meter interne vs Dewesoft: {meter_p.mean():.3f} vs {dew_p.mean():.3f} en moyenne.")

    if detected_summary:
        stats_lines.append(f"CSV Dewesoft detectes: {len(detected_summary.get('dewesoft_csv', []))}")
        stats_lines.append(f"Fichiers Dewesoft bruts: {len(detected_summary.get('dewesoft_raw', []))}")

    timestamps = dew["timestamp"].dropna()
    if not timestamps.empty:
        stats_lines.append(f"Periode couverte: {timestamps.min().isoformat()} -> {timestamps.max().isoformat()}")

    return f"<ul>{_to_list_html(stats_lines)}</ul>"


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

    cause = str(diagnostic.get("cause_probable", "indetermine")).lower()
    cause_label = _label_cause(cause)
    confidence = str(diagnostic.get("confidence", "Faible"))
    confidence_score = int(diagnostic.get("confidence_score", 0))
    justification = str(diagnostic.get("justification", "Aucune justification disponible."))
    executive_summary = str(diagnostic.get("executive_summary", ""))
    best_lead = _label_cause(diagnostic.get("best_lead", "indetermine"))
    best_lead_reason = str(diagnostic.get("best_lead_reason", ""))
    issue_origin = diagnostic.get("issue_origin", {}) or {}

    requested_lines = blocks.get("A_requested", [])
    station_lines = blocks.get("B_station_computed", [])
    protocol_lines = blocks.get("C_sent_to_vehicle", [])
    measured_lines = blocks.get("D_measured", [])
    anomaly_lines = blocks.get("E_anomalies", [])
    insight_lines = cross.get("insights", [])
    evidence_lines = diagnostic.get("evidence", [])
    recommendation_lines = _build_recommendations(diagnostic, detected_summary)

    pcap_total = 0
    dew_total = 0
    if detected_summary:
        pcap_total = len(detected_summary.get("netlogger_pcaps", [])) + len(detected_summary.get("generic_pcaps", []))
        dew_total = len(detected_summary.get("dewesoft_csv", []))

    tone = "warn"
    if cause == "borne":
        tone = "danger"
    elif cause == "communication":
        tone = "info"
    elif cause == "indetermine":
        tone = "neutral"

    verdict_cards_html = "".join(
        [
            _metric_card("Verdict", cause_label, tone=tone),
            _metric_card("Piste a verifier d'abord", best_lead, tone="info" if cause == "indetermine" else "neutral"),
            _metric_card("Confiance", f"{confidence} ({confidence_score}%)", tone="neutral"),
            _metric_card("Evenements exploites", str(len(timeline)), tone="neutral"),
            _metric_card("PCAP detectes", str(pcap_total), tone="info"),
            _metric_card("Dewesoft CSV", str(dew_total), tone="good" if dew_total else "warn"),
            _metric_card("Preuves structurees", str(len(evidence_rows)), tone="neutral"),
        ]
    )

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
          :root {{
            --bg: #eef2f7;
            --surface: #ffffff;
            --surface-alt: #f8fafc;
            --line: #d9e2ec;
            --text: #122033;
            --muted: #566579;
            --accent: #0f766e;
            --accent-dark: #124e66;
            --danger: #b42318;
            --warn: #b54708;
            --info: #175cd3;
            --good: #027a48;
            --shadow: 0 20px 50px rgba(15, 23, 42, 0.08);
          }}
          * {{
            box-sizing: border-box;
          }}
          body {{
            margin: 0;
            padding: 28px;
            background: radial-gradient(circle at top, #f8fbff 0%, var(--bg) 52%);
            color: var(--text);
            font-family: "Segoe UI", Arial, sans-serif;
          }}
          .page {{
            max-width: 1200px;
            margin: 0 auto;
          }}
          .cover {{
            background: linear-gradient(145deg, var(--accent-dark), #0b4f59 55%, #083344);
            color: white;
            border-radius: 28px;
            padding: 30px 34px;
            box-shadow: var(--shadow);
            position: relative;
            overflow: hidden;
          }}
          .cover::after {{
            content: "";
            position: absolute;
            right: -70px;
            top: -60px;
            width: 220px;
            height: 220px;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.08);
          }}
          .eyebrow {{
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            opacity: 0.82;
            margin-bottom: 10px;
          }}
          h1 {{
            margin: 0;
            font-size: 36px;
            line-height: 1.06;
          }}
          .subtitle {{
            max-width: 850px;
            margin-top: 14px;
            font-size: 15px;
            line-height: 1.65;
            color: rgba(255, 255, 255, 0.92);
          }}
          .stamp-row {{
            display: grid;
            grid-template-columns: 1.4fr 1fr;
            gap: 18px;
            margin-top: 24px;
          }}
          .stamp {{
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.12);
            border-radius: 18px;
            padding: 16px 18px;
            backdrop-filter: blur(6px);
          }}
          .stamp h3 {{
            margin: 0 0 6px 0;
            font-size: 12px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
          }}
          .stamp .value {{
            font-size: 28px;
            font-weight: 700;
            margin: 0;
          }}
          .stamp .small {{
            font-size: 13px;
            line-height: 1.55;
            opacity: 0.92;
          }}
          .panel {{
            background: var(--surface);
            border-radius: 22px;
            box-shadow: var(--shadow);
            padding: 24px;
            margin-top: 18px;
          }}
          .panel h2 {{
            margin: 0 0 14px 0;
            font-size: 22px;
          }}
          .panel h3 {{
            margin: 0 0 10px 0;
            font-size: 16px;
          }}
          .panel p {{
            line-height: 1.7;
          }}
          .two-col {{
            display: grid;
            grid-template-columns: 1.2fr 0.8fr;
            gap: 18px;
          }}
          .three-col {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 18px;
          }}
          .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(6, minmax(0, 1fr));
            gap: 14px;
          }}
          .metric-card {{
            background: var(--surface-alt);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 16px;
            min-height: 112px;
          }}
          .metric-label {{
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
            margin-bottom: 10px;
          }}
          .metric-value {{
            font-size: 24px;
            line-height: 1.2;
            font-weight: 700;
          }}
          .tone-danger {{
            border-color: rgba(180, 35, 24, 0.18);
            background: #fff3f2;
          }}
          .tone-warn {{
            border-color: rgba(181, 71, 8, 0.18);
            background: #fff7ed;
          }}
          .tone-info {{
            border-color: rgba(23, 92, 211, 0.18);
            background: #eff8ff;
          }}
          .tone-good {{
            border-color: rgba(2, 122, 72, 0.18);
            background: #ecfdf3;
          }}
          .summary-box {{
            padding: 18px;
            border-radius: 18px;
            background: linear-gradient(180deg, #f8fafc, #f1f5f9);
            border: 1px solid var(--line);
          }}
          .summary-box strong {{
            display: block;
            font-size: 14px;
            margin-bottom: 8px;
          }}
          .step-list {{
            display: grid;
            gap: 14px;
          }}
          .step {{
            border: 1px solid var(--line);
            background: var(--surface-alt);
            border-radius: 18px;
            padding: 16px;
          }}
          .step .step-title {{
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
            margin-bottom: 10px;
          }}
          ul {{
            margin: 0;
            padding-left: 18px;
          }}
          li {{
            margin-bottom: 8px;
            line-height: 1.6;
          }}
          .report-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
          }}
          .report-table th,
          .report-table td {{
            border: 1px solid var(--line);
            padding: 9px 10px;
            vertical-align: top;
          }}
          .report-table th {{
            background: #edf4ff;
            text-align: left;
          }}
          .empty {{
            color: var(--muted);
            font-style: italic;
          }}
          .tag {{
            display: inline-block;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 600;
            background: #ecfdf3;
            color: #027a48;
            margin-right: 8px;
            margin-bottom: 8px;
          }}
          .tag.warn {{
            background: #fff7ed;
            color: #b54708;
          }}
          .tag.info {{
            background: #eff8ff;
            color: #175cd3;
          }}
          @media (max-width: 980px) {{
            body {{
              padding: 16px;
            }}
            .stamp-row,
            .two-col,
            .three-col,
            .metrics-grid {{
              grid-template-columns: 1fr;
            }}
          }}
          @media print {{
            body {{
              background: white;
              padding: 0;
            }}
            .page {{
              max-width: none;
            }}
            .cover,
            .panel {{
              box-shadow: none;
            }}
          }}
        </style>
      </head>
      <body>
        <div class="page">
          <section class="cover">
            <div class="eyebrow">V2G Debug Report</div>
            <h1>Rapport d'analyse multi-sources</h1>
            <div class="subtitle">
              Rapport automatique organise pour reproduire une lecture de debug metier:
              consigne demandee, reponse borne, echanges protocole, mesures internes, mesures Dewesoft
              et verdict probable sur l'origine de l'ecart.
            </div>
            <div class="stamp-row">
              <div class="stamp">
                <h3>Verdict principal</h3>
                <p class="value">{escape(cause_label)}</p>
                <div class="small">Confiance {escape(confidence)} ({escape(str(confidence_score))}%)</div>
              </div>
              <div class="stamp">
                <h3>Generation</h3>
                <div class="small">
                  Genere le {escape(datetime.now(timezone.utc).isoformat())}<br>
                  Session analysee sur base logs + PCAP + Dewesoft quand disponible
                </div>
              </div>
            </div>
          </section>

          <section class="panel">
            <h2>Synthese executive</h2>
            <div class="two-col">
              <div class="summary-box">
                <strong>Lecture rapide</strong>
                <p>{escape(executive_summary)}</p>
              </div>
              <div class="summary-box">
                <strong>Pourquoi ce verdict</strong>
                <p>{escape(justification)}</p>
              </div>
            </div>
            <div class="metrics-grid" style="margin-top: 18px;">
              {verdict_cards_html}
            </div>
          </section>

          <section class="panel">
            <h2>Cadre d'analyse</h2>
            <div class="three-col">
              <div class="summary-box">
                <strong>Objectif du debug</strong>
                <p>Determiner si l'ecart principal vient de la borne, du vehicule, de la communication, ou si les preuves restent insuffisantes.</p>
              </div>
              <div class="summary-box">
                <strong>Donnees disponibles</strong>
                <ul>{_to_list_html(_build_file_summary(detected_summary))}</ul>
              </div>
              <div class="summary-box">
                <strong>Actions recommandees</strong>
                <ul>{_to_list_html(recommendation_lines)}</ul>
              </div>
            </div>
          </section>

          <section class="panel">
            <h2>Point de depart probable</h2>
            <div class="two-col">
              <div class="summary-box">
                <strong>Moment et source a verifier</strong>
                <p>
                  {escape(str(issue_origin.get("timestamp") or "Horodatage non determine"))}<br>
                  {escape(str(issue_origin.get("source") or "Source non determinee"))}
                </p>
              </div>
              <div class="summary-box">
                <strong>Interpretation</strong>
                <p>{escape(str(issue_origin.get("reason") or "Point de depart du probleme non determine."))}</p>
                <p><b>Piste principale:</b> {escape(best_lead)}</p>
                <p>{escape(best_lead_reason)}</p>
              </div>
            </div>
          </section>

          <section class="panel">
            <h2>Matrice des sources</h2>
            <p>Cette matrice rappelle le role de chaque famille de donnees dans la justification finale.</p>
            {_source_matrix_html(detected_summary)}
          </section>

          <section class="panel">
            <h2>Resume de session</h2>
            <ul>{_to_list_html(summary_lines)}</ul>
          </section>

          <section class="panel">
            <h2>Chemin de decision</h2>
            <div class="step-list">
              <div class="step">
                <div class="step-title">1. Ce qui a ete demande</div>
                <ul>{_to_list_html(requested_lines)}</ul>
              </div>
              <div class="step">
                <div class="step-title">2. Ce que la borne a calcule ou publie</div>
                <ul>{_to_list_html(station_lines)}</ul>
              </div>
              <div class="step">
                <div class="step-title">3. Ce qui a ete envoye au vehicule</div>
                <ul>{_to_list_html(protocol_lines)}</ul>
              </div>
              <div class="step">
                <div class="step-title">4. Ce qui a ete mesure</div>
                <ul>{_to_list_html(measured_lines)}</ul>
              </div>
              <div class="step">
                <div class="step-title">5. Anomalies retenues</div>
                <ul>{_to_list_html(anomaly_lines)}</ul>
              </div>
            </div>
          </section>

          <section class="panel">
            <h2>Conclusion motivee</h2>
            <div style="margin-bottom: 10px;">
              <span class="tag">Verdict: {escape(cause_label)}</span>
              <span class="tag warn">Confiance: {escape(confidence)} ({escape(str(confidence_score))}%)</span>
              <span class="tag info">Preuves: {escape(str(len(evidence_lines)))}</span>
            </div>
            <div class="two-col">
              <div class="summary-box">
                <strong>Preuves fortes</strong>
                <ul>{_to_list_html(evidence_lines)}</ul>
              </div>
              <div class="summary-box">
                <strong>Donnees manquantes</strong>
                <ul>{_to_list_html(diagnostic.get("missing_data", []), empty_label="Aucune donnee critique manquante detectee")}</ul>
              </div>
            </div>
          </section>

          <section class="panel">
            <h2>Correlation inter-sources</h2>
            <p>
              Cette section aligne les consignes, les messages protocoles, les mesures internes et les mesures Dewesoft.
              Elle doit permettre a un lecteur humain de verifier rapidement si l'ecart est visible et sur quelle source il apparait.
            </p>
            <h3>Ecarts detectes</h3>
            <ul>{_to_list_html(insight_lines)}</ul>
            {cross_table_html}
          </section>

          <section class="panel">
            <h2>Analyse Dewesoft</h2>
            {_dewesoft_realtime_section(timeline, detected_summary)}
          </section>

          <section class="panel">
            <h2>Preuves structurees</h2>
            {evidence_table_html}
          </section>

          <section class="panel">
            <h2>Timeline de reference</h2>
            {timeline_html}
          </section>
        </div>
      </body>
    </html>
    """
