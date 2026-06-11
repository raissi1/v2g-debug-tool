"""Streamlit entrypoint for the V2G generic debug tool."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any


def _bootstrap_import_paths() -> None:
    script_path = Path(__file__).resolve()
    candidates = [script_path.parents[1], Path.cwd().resolve(), script_path.parent]
    for candidate in candidates:
        if candidate.exists() and (candidate / "analyzers").exists() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))


_bootstrap_import_paths()

import pandas as pd

from analyzers.diagnostic_engine import run_diagnostic
from analyzers.generic_debug import summarize_session
from core.session_builder import build_session_timeline
from graphs.plot_builder import build_signal_figure
from reports.html_report import generate_html_report
from reports.pdf_report import generate_pdf_report
from timeline.reconstructor import build_timeseries_view
from utils.file_detector import detect_session_files
from utils.zip_loader import extract_zip_to_temp


CAUSE_LABELS = {
    "borne": "Cote borne",
    "vehicule": "Cote vehicule",
    "communication": "Cote communication",
    "indetermine": "Indetermine",
}


def _resolve_input_source(
    input_mode: str,
    folder_path: str,
    uploaded_zip: Any,
) -> tuple[Path, tempfile.TemporaryDirectory[str] | None]:
    if input_mode == "Dossier local":
        if not folder_path:
            raise ValueError("Veuillez indiquer un chemin local vers un dossier de session.")
        session_dir = Path(folder_path).expanduser().resolve()
        if not session_dir.is_dir():
            raise ValueError(f"Dossier introuvable: {session_dir}")
        return session_dir, None

    if uploaded_zip is None:
        raise ValueError("Veuillez charger un fichier ZIP de session.")

    temp_dir = tempfile.TemporaryDirectory(prefix="v2g_session_")
    zip_path = Path(temp_dir.name) / uploaded_zip.name
    with zip_path.open("wb") as out:
        out.write(uploaded_zip.getvalue())

    extracted_dir = extract_zip_to_temp(zip_path, Path(temp_dir.name))
    return extracted_dir, temp_dir


def _compute_overview_metrics(session_df: pd.DataFrame, detected_summary: dict) -> dict[str, int]:
    pcap_count = len(detected_summary.get("netlogger_pcaps", [])) + len(detected_summary.get("generic_pcaps", []))
    if session_df.empty:
        return {
            "files_analyzed": 0,
            "events": 0,
            "errors": 0,
            "warnings": 0,
            "gridcodes": 0,
            "setpoints": 0,
            "pcaps": pcap_count,
            "measures": len(detected_summary.get("dewesoft_csv", [])) + len(detected_summary.get("dewesoft_raw", [])),
        }

    event_counts = session_df["event_type"].value_counts()
    files_count = (
        len(detected_summary.get("energy_manager", []))
        + len(detected_summary.get("charger_app", []))
        + len(detected_summary.get("iotc_meter_dispatcher", []))
        + pcap_count
        + len(detected_summary.get("netlogger_logs", []))
        + len(detected_summary.get("dewesoft_csv", []))
        + len(detected_summary.get("dewesoft_raw", []))
        + len(detected_summary.get("generic_logs", []))
    )
    return {
        "files_analyzed": files_count,
        "events": int(len(session_df)),
        "errors": int(event_counts.get("error", 0)),
        "warnings": int(event_counts.get("warning", 0)),
        "gridcodes": int(event_counts.get("gridcodes", 0)),
        "setpoints": int(event_counts.get("setpoint", 0)),
        "pcaps": pcap_count,
        "measures": len(detected_summary.get("dewesoft_csv", [])) + len(detected_summary.get("dewesoft_raw", [])),
    }


def _label_cause(value: str) -> str:
    return CAUSE_LABELS.get(str(value).lower(), str(value))


def _label_classifier_confidence(value: str) -> str:
    mapping = {
        "HIGH": "Elevee",
        "MEDIUM": "Moyenne",
        "LOW": "Faible",
        "INDETERMINATE": "Indeterminee",
    }
    return mapping.get(str(value or "").upper(), str(value or "Indeterminee"))


def _inject_premium_css(st) -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at 14% 12%, rgba(13, 148, 136, 0.15), transparent 26%),
                radial-gradient(circle at 88% 10%, rgba(14, 165, 233, 0.12), transparent 24%),
                linear-gradient(180deg, #f4f8fc 0%, #ebf2f8 52%, #e8eef6 100%);
        }
        .block-container {
            max-width: 1320px;
            padding-top: 2.1rem;
            padding-bottom: 2.5rem;
            padding-left: 2.4rem;
            padding-right: 2.4rem;
        }
        section[data-testid="stSidebar"] {
            background:
                linear-gradient(180deg, rgba(248, 251, 255, 0.97) 0%, rgba(237, 244, 251, 0.98) 100%);
            border-right: 1px solid rgba(15, 23, 42, 0.08);
            backdrop-filter: blur(12px);
        }
        section[data-testid="stSidebar"] > div {
            padding-top: 1.2rem;
        }
        .premium-shell {
            display: grid;
            gap: 1rem;
        }
        .hero-grid {
            display: grid;
            grid-template-columns: minmax(0, 1.65fr) minmax(280px, 0.85fr);
            gap: 18px;
            align-items: stretch;
        }
        .premium-hero {
            position: relative;
            overflow: hidden;
            padding: 30px 32px 32px 32px;
            border-radius: 30px;
            color: white;
            background:
                radial-gradient(circle at top right, rgba(255,255,255,0.18), transparent 28%),
                linear-gradient(135deg, #0f766e 0%, #155e75 48%, #0f172a 100%);
            box-shadow: 0 24px 70px rgba(15, 23, 42, 0.18);
            min-height: 290px;
        }
        .premium-hero::after {
            content: "";
            position: absolute;
            inset: auto -40px -55px auto;
            width: 180px;
            height: 180px;
            background: radial-gradient(circle, rgba(255,255,255,0.22), transparent 70%);
            pointer-events: none;
        }
        .hero-kicker {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 7px 12px;
            border-radius: 999px;
            background: rgba(255,255,255,0.14);
            border: 1px solid rgba(255,255,255,0.18);
            font-size: 0.8rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 18px;
        }
        .premium-hero h1 {
            margin: 0;
            font-size: 3rem;
            line-height: 0.98;
            letter-spacing: -0.03em;
            max-width: 760px;
        }
        .premium-hero p {
            margin: 14px 0 0 0;
            font-size: 1.08rem;
            line-height: 1.65;
            max-width: 920px;
            color: rgba(255,255,255,0.9);
        }
        .hero-band {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 22px;
        }
        .hero-band span {
            padding: 9px 12px;
            border-radius: 999px;
            background: rgba(255,255,255,0.1);
            border: 1px solid rgba(255,255,255,0.14);
            font-size: 0.92rem;
            color: rgba(255,255,255,0.94);
        }
        .hero-aside {
            display: grid;
            gap: 14px;
        }
        .hero-panel {
            background: rgba(255,255,255,0.84);
            border: 1px solid rgba(15,23,42,0.08);
            border-radius: 24px;
            padding: 18px 18px 16px 18px;
            box-shadow: 0 16px 38px rgba(15, 23, 42, 0.08);
            backdrop-filter: blur(8px);
        }
        .hero-panel h3 {
            margin: 0 0 12px 0;
            font-size: 1rem;
            color: #0f172a;
        }
        .hero-panel p {
            margin: 0;
            color: #475569;
            line-height: 1.5;
            font-size: 0.95rem;
        }
        .hero-checklist {
            display: grid;
            gap: 10px;
        }
        .hero-check {
            display: grid;
            grid-template-columns: 28px 1fr;
            gap: 10px;
            align-items: start;
        }
        .hero-check strong {
            display: block;
            color: #0f172a;
            font-size: 0.95rem;
        }
        .hero-check span {
            color: #64748b;
            font-size: 0.88rem;
            line-height: 1.45;
        }
        .hero-dot {
            width: 28px;
            height: 28px;
            border-radius: 50%;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: linear-gradient(135deg, #0f766e, #0ea5e9);
            color: white;
            font-size: 0.85rem;
            font-weight: 700;
        }
        .landing-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 16px;
            margin-top: 4px;
        }
        .premium-strip {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 14px;
            margin: 16px 0 8px 0;
        }
        .premium-card {
            background: rgba(255,255,255,0.92);
            border: 1px solid rgba(15,23,42,0.07);
            border-radius: 18px;
            padding: 16px 18px;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
        }
        .premium-card .label {
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #475569;
            margin-bottom: 8px;
        }
        .premium-card .value {
            font-size: 1.35rem;
            font-weight: 700;
            color: #0f172a;
        }
        .premium-note {
            background: linear-gradient(180deg, rgba(255,249,219,0.96) 0%, rgba(255,242,182,0.94) 100%);
            border: 1px solid rgba(202, 138, 4, 0.14);
            border-radius: 22px;
            padding: 16px 18px;
            color: #854d0e;
            margin: 2px 0 6px 0;
            box-shadow: 0 12px 30px rgba(202, 138, 4, 0.08);
        }
        .premium-note strong {
            display: block;
            color: #713f12;
            margin-bottom: 6px;
            font-size: 0.95rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .landing-card {
            background: rgba(255,255,255,0.9);
            border: 1px solid rgba(15,23,42,0.07);
            border-radius: 22px;
            padding: 18px;
            box-shadow: 0 14px 34px rgba(15, 23, 42, 0.06);
        }
        .landing-card h3 {
            margin: 0 0 10px 0;
            color: #0f172a;
            font-size: 1.05rem;
        }
        .landing-card p {
            margin: 0;
            color: #64748b;
            line-height: 1.55;
            font-size: 0.94rem;
        }
        .empty-state {
            background: linear-gradient(180deg, rgba(248,250,220,0.9) 0%, rgba(241,246,214,0.9) 100%);
            border: 1px solid rgba(163, 230, 53, 0.14);
            border-radius: 22px;
            padding: 18px 20px;
            box-shadow: 0 12px 28px rgba(132, 204, 22, 0.08);
            color: #3f6212;
        }
        .empty-state strong {
            display: block;
            font-size: 1rem;
            margin-bottom: 6px;
            color: #365314;
        }
        [data-testid="stFileUploader"] {
            background: rgba(255,255,255,0.72);
            border: 1px solid rgba(15,23,42,0.08);
            border-radius: 22px;
            padding: 8px;
        }
        [data-testid="stFileUploaderDropzone"] {
            border: 1.5px dashed rgba(14, 116, 144, 0.24);
            border-radius: 18px;
            background: linear-gradient(180deg, rgba(255,255,255,0.72) 0%, rgba(245,250,255,0.94) 100%);
        }
        [data-testid="stFileUploaderDropzone"] * {
            color: #334155;
        }
        [data-testid="stSidebar"] .stButton > button {
            border-radius: 16px;
            min-height: 3rem;
            font-weight: 700;
            font-size: 1.02rem;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.12);
        }
        [data-testid="stSidebar"] .stRadio label,
        [data-testid="stSidebar"] .stTextInput label,
        [data-testid="stSidebar"] .stFileUploader label {
            color: #334155;
        }
        div[data-testid="stMetric"] {
            background: rgba(255,255,255,0.88);
            border: 1px solid rgba(15,23,42,0.06);
            padding: 14px 16px;
            border-radius: 18px;
            box-shadow: 0 8px 24px rgba(15,23,42,0.05);
        }
        @media (max-width: 1100px) {
            .hero-grid,
            .landing-grid,
            .premium-strip {
                grid-template-columns: 1fr;
            }
            .premium-hero h1 {
                font-size: 2.3rem;
            }
            .block-container {
                padding-left: 1.2rem;
                padding-right: 1.2rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_overview_strip(st, detected_summary: dict) -> None:
    pcap_total = len(detected_summary.get("netlogger_pcaps", [])) + len(detected_summary.get("generic_pcaps", []))
    dew_raw = len(detected_summary.get("dewesoft_raw", []))
    dew_csv = len(detected_summary.get("dewesoft_csv", []))
    st.markdown(
        f"""
        <div class="premium-strip">
          <div class="premium-card">
            <div class="label">Sources detectees</div>
            <div class="value">{len(detected_summary.get('energy_manager', [])) + len(detected_summary.get('charger_app', [])) + len(detected_summary.get('iotc_meter_dispatcher', []))} logs metier</div>
          </div>
          <div class="premium-card">
            <div class="label">Dewesoft</div>
            <div class="value">{dew_csv} CSV / {dew_raw} bruts</div>
          </div>
          <div class="premium-card">
            <div class="label">Protocoles</div>
            <div class="value">{pcap_total} PCAP detectes</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_detected_files(st, detected_summary: dict) -> None:
    pcap_total = len(detected_summary.get("netlogger_pcaps", [])) + len(detected_summary.get("generic_pcaps", []))
    coverage = detected_summary.get("coverage", {})
    dewesoft_coverage = coverage.get("dewesoft", {})
    st.write(f"EnergyManager: **{len(detected_summary.get('energy_manager', []))}**")
    st.write(f"ChargerApp: **{len(detected_summary.get('charger_app', []))}**")
    st.write(f"iotc-meter-dispatcher: **{len(detected_summary.get('iotc_meter_dispatcher', []))}**")
    st.write(f"PCAP detectes: **{pcap_total}**")
    st.write(f"Mesures Dewesoft CSV: **{len(detected_summary.get('dewesoft_csv', []))}**")
    st.write(f"Mesures Dewesoft brutes (.d7d/.dxd/.dmd): **{len(detected_summary.get('dewesoft_raw', []))}**")
    st.write(f"Captures et images detectees: **{len(detected_summary.get('supporting_images', []))}**")
    if dewesoft_coverage:
        st.write(
            "Statut Dewesoft: "
            f"**{dewesoft_coverage.get('csv_ready', 0)} CSV prets**, "
            f"**{dewesoft_coverage.get('sidecar_csv', 0)} bruts associes a un CSV**, "
            f"**{dewesoft_coverage.get('conversion_required', 0)} a convertir**"
        )
    if detected_summary.get("dewesoft_raw", []) and not detected_summary.get("dewesoft_csv", []):
        st.warning("Dewesoft brut detecte: present dans la session, mais conversion CSV requise pour exploiter les mesures.")


def _analysis_status_text(step: str) -> str:
    labels = {
        "resolve": "Preparation de la session...",
        "detect": "Detection des fichiers sources...",
        "timeline": "Reconstruction de la timeline...",
        "timeseries": "Construction des signaux physiques...",
        "summary": "Generation du resume automatique...",
        "diagnostic": "Execution du diagnostic...",
        "finalize": "Finalisation du rapport et du dashboard...",
    }
    return labels.get(step, "Analyse en cours...")


def _extract_pcap_diagnostics(session_df: pd.DataFrame) -> list[dict[str, Any]]:
    if session_df.empty or "payload" not in session_df.columns:
        return []

    rows: list[dict[str, Any]] = []
    for _, row in session_df.iterrows():
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
        if payload.get("parser") != "pcap_generic":
            continue
        rows.append(
            {
                "timestamp": row.get("timestamp"),
                "source": row.get("source"),
                "message": row.get("message"),
                "packets": payload.get("pcap_packet_count"),
                "linktype": payload.get("pcap_link_type"),
                "ports": payload.get("pcap_top_ports"),
                "tcp_resets": payload.get("pcap_tcp_rst_count"),
                "homeplug": payload.get("pcap_has_homeplug"),
                "ipv6": payload.get("pcap_has_ipv6"),
                "tcp": payload.get("pcap_has_tcp"),
                "likely_v2g": payload.get("pcap_likely_v2g"),
                "markers": payload.get("pcap_markers"),
            }
        )
    return rows


def _render_landing_overview(st) -> None:
    st.markdown(
        """
        <div class="premium-shell">
          <div class="hero-grid">
            <section class="premium-hero">
              <div class="hero-kicker">Plateforme de diagnostic V2G</div>
              <h1>V2G Session Debugger</h1>
              <p>
                Analyse multi-sources plus lisible, plus convaincante et plus exploitable:
                on aligne les logs borne, les PCAP, les acquisitions Dewesoft et les captures
                pour localiser l'ecart et produire un rapport presentable.
              </p>
              <div class="hero-band">
                <span>Logs borne structurés</span>
                <span>PCAP interpretés</span>
                <span>Dewesoft comparé aux consignes</span>
                <span>Rapport HTML + PDF</span>
              </div>
            </section>
            <div class="hero-aside">
              <section class="hero-panel">
                <h3>Ce que l'outil sait faire</h3>
                <div class="hero-checklist">
                  <div class="hero-check">
                    <div class="hero-dot">1</div>
                    <div><strong>Reconstruire la session</strong><span>Fusion des evenements metier, signaux physiques et indices protocole.</span></div>
                  </div>
                  <div class="hero-check">
                    <div class="hero-dot">2</div>
                    <div><strong>Identifier le premier ecart</strong><span>Repere le moment ou la consigne, la borne et la mesure ne racontent plus la meme histoire.</span></div>
                  </div>
                  <div class="hero-check">
                    <div class="hero-dot">3</div>
                    <div><strong>Sortir un verdict motive</strong><span>Cause probable, niveau de confiance, preuves et recommandations client.</span></div>
                  </div>
                </div>
              </section>
              <section class="hero-panel">
                <h3>Formats attendus</h3>
                <p>ZIP de session avec logs, dossiers PCAP, exports Dewesoft CSV, fichiers bruts Dewesoft et captures de support.</p>
              </section>
            </div>
          </div>
          <div class="premium-note">
            <strong>Workflow metier</strong>
            Importer une session, detecter les sources, reconstruire la timeline, correler les preuves, puis formuler un verdict motive exploitable en revue technique ou client.
          </div>
          <div class="landing-grid">
            <section class="landing-card">
              <h3>Lecture orientee diagnostic</h3>
              <p>L'interface met en avant le point de depart probable, le premier point de divergence et les regles generiques qui expliquent la conclusion.</p>
            </section>
            <section class="landing-card">
              <h3>Preuves reseau et energie</h3>
              <p>Les PCAP remontent des marqueurs V2G, des ports, des resets TCP et du HomePlug/SLAC. Les mesures Dewesoft servent de reference physique.</p>
            </section>
            <section class="landing-card">
              <h3>Rapport presentable</h3>
              <p>Le tableau de bord et les exports HTML/PDF sont pensés pour servir de base a une vraie restitution, pas seulement a un debug brut.</p>
            </section>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def run_streamlit_app() -> None:
    try:
        import streamlit as st
        import streamlit.components.v1 as components
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Streamlit n'est pas installe. Executez `pip install -r requirements.txt` "
            "puis lancez `streamlit run app/main.py`."
        ) from exc

    st.set_page_config(page_title="V2G Session Debugger", layout="wide")
    _inject_premium_css(st)
    _render_landing_overview(st)

    if "analysis" not in st.session_state:
        st.session_state.analysis = None

    with st.sidebar:
        st.header("Entree session")
        input_mode = st.radio("Type de source", ["Fichier ZIP", "Dossier local"], index=0)
        folder_path = ""
        uploaded_zip = None

        if input_mode == "Dossier local":
            folder_path = st.text_input("Chemin dossier session", value="", placeholder="C:/.../session")
        else:
            uploaded_zip = st.file_uploader("Session ZIP", type=["zip"])
            if uploaded_zip is not None:
                size_mb = uploaded_zip.size / (1024 * 1024)
                st.success(f"ZIP charge: {uploaded_zip.name} ({size_mb:.1f} MB)")

        st.caption("Le ZIP peut contenir les logs borne, les traces PCAP et les exports Dewesoft CSV.")
        analyze_clicked = st.button("Analyser la session", type="primary", width="stretch")

        if st.session_state.analysis is not None:
            _render_overview_strip(st, st.session_state.analysis["detected_summary"])
            st.markdown("### Sources detectees")
            _render_detected_files(st, st.session_state.analysis["detected_summary"])
            with st.expander("Voir le detail des fichiers detectes"):
                st.json(st.session_state.analysis["detected_summary"])

    if analyze_clicked:
        temp_dir: tempfile.TemporaryDirectory[str] | None = None
        progress_box = st.empty()
        progress_bar = st.progress(0, text=_analysis_status_text("resolve"))
        try:
            progress_bar.progress(10, text=_analysis_status_text("resolve"))
            session_dir, temp_dir = _resolve_input_source(input_mode, folder_path, uploaded_zip)

            progress_bar.progress(25, text=_analysis_status_text("detect"))
            detected = detect_session_files(session_dir)

            progress_bar.progress(45, text=_analysis_status_text("timeline"))
            session_df = build_session_timeline(detected)

            progress_bar.progress(60, text=_analysis_status_text("timeseries"))
            timeseries = build_timeseries_view(session_df)

            progress_bar.progress(72, text=_analysis_status_text("summary"))
            summary_lines = summarize_session(session_df)

            progress_bar.progress(88, text=_analysis_status_text("diagnostic"))
            diagnostic = run_diagnostic(session_df)
            detected_summary = detected.to_summary()

            progress_bar.progress(100, text=_analysis_status_text("finalize"))
            st.session_state.analysis = {
                "session_df": session_df,
                "timeseries": timeseries,
                "summary_lines": summary_lines,
                "diagnostic": diagnostic,
                "detected_summary": detected_summary,
            }
            progress_box.success(
                "Analyse terminee. "
                f"Verdict: {_label_cause(diagnostic.get('cause_probable', 'indetermine'))} | "
                f"confiance {diagnostic.get('confidence', 'Faible')} ({diagnostic.get('confidence_score', 0)}%)."
            )
        except Exception as exc:  # noqa: BLE001
            progress_bar.empty()
            progress_box.error("Analyse interrompue. Voir le detail de l'erreur ci-dessous.")
            st.error(str(exc))
            st.exception(exc)
            st.session_state.analysis = None
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()

    analysis = st.session_state.analysis
    if analysis is None:
        st.markdown(
            """
            <div class="empty-state">
              <strong>Aucune analyse disponible</strong>
              Chargez une session a gauche, puis lancez l'analyse pour afficher le verdict,
              les preuves inter-sources, le diagnostic PCAP et les rapports exportables.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    session_df: pd.DataFrame = analysis["session_df"]
    timeseries: pd.DataFrame = analysis["timeseries"]
    summary_lines: list[str] = analysis["summary_lines"]
    diagnostic: dict = analysis["diagnostic"]
    detected_summary: dict = analysis["detected_summary"]
    metrics = _compute_overview_metrics(session_df, detected_summary)
    report_html = generate_html_report(summary_lines, diagnostic, session_df, detected_summary)
    report_pdf = generate_pdf_report(summary_lines, diagnostic, session_df, detected_summary)

    tabs = st.tabs(["Verdict", "Sources", "Timeline", "Graphes", "Preuves", "Rapport"])

    with tabs[0]:
        c1, c2, c3, c4 = st.columns(4)
        c5, c6, c7, c8 = st.columns(4)
        c1.metric("Fichiers analyses", metrics["files_analyzed"])
        c2.metric("Evenements", metrics["events"])
        c3.metric("Erreurs", metrics["errors"])
        c4.metric("Warnings", metrics["warnings"])
        c5.metric("GridCodes", metrics["gridcodes"])
        c6.metric("Setpoints", metrics["setpoints"])
        c7.metric("PCAP detectes", metrics["pcaps"])
        c8.metric("Mesures detectees", metrics["measures"])

        cause = diagnostic.get("cause_probable", "indetermine")
        confidence = diagnostic.get("confidence_score", 0)
        confidence_label = diagnostic.get("confidence", "Faible")
        verdict_label = _label_cause(cause)

        st.subheader("Verdict principal")
        if cause == "borne":
            st.error(f"{verdict_label} | confiance {confidence_label} ({confidence}%)")
        elif cause == "vehicule":
            st.warning(f"{verdict_label} | confiance {confidence_label} ({confidence}%)")
        elif cause == "communication":
            st.info(f"{verdict_label} | confiance {confidence_label} ({confidence}%)")
        else:
            st.info(f"{verdict_label} | confiance {confidence_label} ({confidence}%)")

        st.markdown("### Lecture metier")
        st.write(diagnostic.get("justification", "Aucune justification disponible."))
        st.write(diagnostic.get("executive_summary", ""))
        st.markdown("### Classification V2G")
        v2g_classification = diagnostic.get("v2g_classification", {}) or {}
        classifier_cause = _label_cause(v2g_classification.get("cause", "indetermine"))
        classifier_confidence = _label_classifier_confidence(v2g_classification.get("confidence", "INDETERMINATE"))
        classifier_score = int(v2g_classification.get("confidence_score", 0) or 0)
        st.write(
            f"Verdict V2G cible: **{classifier_cause}** | "
            f"confiance **{classifier_confidence} ({classifier_score}%)**"
        )
        st.write(v2g_classification.get("justification", "Aucune justification specifique disponible."))
        score_c1, score_c2, score_c3 = st.columns(3)
        score_c1.metric("Score borne", v2g_classification.get("borne_score", 0))
        score_c2.metric("Score vehicule", v2g_classification.get("vehicule_score", 0))
        score_c3.metric("Score communication", v2g_classification.get("communication_score", 0))

        classifier_evidence = v2g_classification.get("evidence", []) or []
        if classifier_evidence:
            st.markdown("### Preuves V2G retenues")
            for item in classifier_evidence[:8]:
                lead = item.get("message") or item.get("signal") or "Preuve"
                side = _label_cause(item.get("side", "indetermine"))
                details = item.get("details") or []
                detail_text = f" | details: {', '.join(str(part) for part in details[:3])}" if details else ""
                ts = item.get("timestamp") or "horodatage non disponible"
                st.write(f"- {side} | {ts} | {lead}{detail_text}")

        recommendations = v2g_classification.get("recommendations", []) or []
        if recommendations:
            st.markdown("### Recommandations V2G")
            for recommendation in recommendations:
                st.write(f"- {recommendation}")

        st.markdown("### Point de depart probable")
        issue_origin = diagnostic.get("issue_origin", {}) or {}
        first_divergence = diagnostic.get("first_divergence", {}) or {}
        lead_label = _label_cause(diagnostic.get("best_lead", "indetermine"))
        st.write(f"Piste a verifier d'abord: **{lead_label}**")
        st.write(diagnostic.get("best_lead_reason", ""))
        st.write(
            f"Moment suspect: **{issue_origin.get('timestamp') or 'non determine'}** | "
            f"Source: **{issue_origin.get('source') or 'non determinee'}**"
        )
        st.write(issue_origin.get("reason", "Point de depart du probleme non determine."))
        st.markdown("### Premier point de divergence")
        st.write(
            f"Horodatage: **{first_divergence.get('timestamp') or 'non determine'}** | "
            f"Source: **{first_divergence.get('source') or 'non determinee'}** | "
            f"Categorie: **{first_divergence.get('category') or 'indetermine'}**"
        )
        st.write(first_divergence.get("reason", "Aucun point de divergence net n'a ete determine."))
        st.markdown("### Regles generiques")
        rule_summary = diagnostic.get("generic_rule_summary", {}) or {}
        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
        col_r1.metric("Pass", rule_summary.get("pass", 0))
        col_r2.metric("Warn", rule_summary.get("warn", 0))
        col_r3.metric("Fail", rule_summary.get("fail", 0))
        col_r4.metric("Unknown", rule_summary.get("unknown", 0))

        st.markdown("### Resume automatique")
        if session_df.empty:
            st.info("Aucun evenement exploitable detecte dans la session.")
        else:
            readable_summary = (
                f"La session contient {metrics['events']} evenements, avec {metrics['errors']} erreurs et "
                f"{metrics['warnings']} warnings. {metrics['setpoints']} changements de consigne ont ete detectes, "
                f"ainsi que {metrics['gridcodes']} evenements GridCodes. "
                f"Les sources disponibles incluent {len(detected_summary.get('energy_manager', []))} fichier(s) EnergyManager, "
                f"{len(detected_summary.get('charger_app', []))} fichier(s) ChargerApp et "
                f"{len(detected_summary.get('iotc_meter_dispatcher', []))} fichier(s) meter dispatcher."
            )
            st.write(readable_summary)

    with tabs[1]:
        st.subheader("Inventaire des donnees detectees")
        _render_overview_strip(st, detected_summary)
        _render_detected_files(st, detected_summary)

        st.markdown("### Detail des types de donnees")
        st.write("Logs borne: utilises pour reconstruire la consigne, les recalculs, les limitations et les erreurs.")
        st.write("PCAP: utilises pour confirmer les echanges protocole quand ils sont exploitables dans la timeline.")
        st.write("Dewesoft CSV: utilises pour comparer les mesures physiques reelles avec les consignes et le meter interne.")
        st.write("Captures: utiles pour documenter visuellement les transitions de consigne, mesures Primara et extractions protocole.")

        st.markdown("### Diagnostic PCAP")
        pcap_diagnostics = _extract_pcap_diagnostics(session_df)
        if pcap_diagnostics:
            st.dataframe(pd.DataFrame(pcap_diagnostics), width="stretch")
        else:
            st.info("Aucun diagnostic PCAP detaille disponible.")

        with st.expander("Voir le dictionnaire complet de detection"):
            st.json(detected_summary)

    with tabs[2]:
        st.subheader("Timeline filtrable")
        if session_df.empty:
            st.info("Timeline vide.")
        else:
            sources = sorted(session_df["source"].dropna().unique().tolist()) if "source" in session_df.columns else []
            event_types = sorted(session_df["event_type"].dropna().unique().tolist()) if "event_type" in session_df.columns else []

            col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
            source_filter = col_f1.multiselect("Source", sources)
            event_filter = col_f2.multiselect("Type d'evenement", event_types)
            text_query = col_f3.text_input("Recherche texte", value="")

            filtered = session_df.copy()
            if source_filter:
                filtered = filtered[filtered["source"].isin(source_filter)]
            if event_filter:
                filtered = filtered[filtered["event_type"].isin(event_filter)]
            if text_query:
                filtered = filtered[filtered["message"].astype(str).str.contains(text_query, case=False, na=False)]

            visible_columns = [column for column in ["timestamp", "source", "event_type", "message", "interpretation"] if column in filtered.columns]
            st.dataframe(filtered[visible_columns], width="stretch")

    with tabs[3]:
        st.subheader("Graphes physiques")
        has_measure = not timeseries.empty and any(
            column in timeseries.columns and pd.to_numeric(timeseries[column], errors="coerce").notna().any()
            for column in [
                "Ptarget",
                "P_meter",
                "P_dewesoft",
                "Qtarget",
                "Q_meter",
                "Q_dewesoft",
                "U_meter",
                "U_dewesoft",
                "frequency_meter",
                "frequency_dewesoft",
            ]
        )
        if has_measure:
            st.plotly_chart(build_signal_figure(timeseries, diagnostic.get("first_divergence")), width="stretch")
        else:
            st.info("Aucune mesure exploitable detectee. Importez un export CSV Dewesoft pour activer les graphes physiques.")

    with tabs[4]:
        st.subheader("Preuves et anomalies")

        st.markdown("### Ecarts cles")
        insights = diagnostic.get("cross_analysis", {}).get("insights", [])
        if insights:
            for insight in insights:
                st.write(f"- {insight}")
        else:
            st.write("Aucun ecart fort remonte par la correlation inter-sources.")

        st.markdown("### Preuves utilisees")
        for evidence in diagnostic.get("evidence", []):
            st.write(f"- {evidence}")

        st.markdown("### Donnees manquantes")
        missing_data = diagnostic.get("missing_data", [])
        st.write(", ".join(missing_data) if missing_data else "Aucune donnee critique manquante detectee.")

        st.markdown("### Anomalies detectees")
        anomaly_types = ["error", "warning", "power_limit", "timeout", "protocol_event", "gridcodes"]
        anomalies = session_df[session_df["event_type"].isin(anomaly_types)].copy() if not session_df.empty else pd.DataFrame()
        if anomalies.empty:
            st.success("Aucune anomalie majeure detectee dans les categories surveillees.")
        else:
            st.dataframe(
                anomalies[[column for column in ["timestamp", "source", "event_type", "message"] if column in anomalies.columns]],
                width="stretch",
            )

        st.markdown("### Lecture protocolaire")
        pcap_diagnostics = _extract_pcap_diagnostics(session_df)
        if pcap_diagnostics:
            pcap_frame = pd.DataFrame(pcap_diagnostics)
            best = pcap_frame.iloc[0].to_dict()
            st.write(
                f"PCAP reconnu sur **{best.get('source') or 'source inconnue'}** avec "
                f"**{best.get('packets') or 0}** paquets, ports **{best.get('ports') or []}**, "
                f"V2G probable: **{best.get('likely_v2g')}**, resets TCP: **{best.get('tcp_resets')}**."
            )
            markers = best.get("markers") or []
            if markers:
                st.write("Marqueurs detectes: " + ", ".join(str(marker) for marker in markers[:6]))
        else:
            st.info("Aucune lecture protocolaire detaillee n'a ete extraite des PCAP.")

        st.markdown("### Dewesoft")
        csv_count = len(detected_summary.get("dewesoft_csv", []))
        raw_count = len(detected_summary.get("dewesoft_raw", []))
        if csv_count > 0:
            st.write(f"Mesures CSV disponibles: {csv_count}")
        elif raw_count > 0:
            st.warning(
                f"{raw_count} fichier(s) Dewesoft .d7d/.dxd/.dmd detecte(s): presents dans la session, "
                "mais conversion CSV requise pour analyse detaillee."
            )
        else:
            st.info("Aucune acquisition Dewesoft detectee.")

        st.markdown("### Regles d'analyse generiques")
        generic_rules = diagnostic.get("generic_rules", []) or []
        if generic_rules:
            st.dataframe(pd.DataFrame(generic_rules), width="stretch")
        else:
            st.info("Aucune regle generique evaluee.")

    with tabs[5]:
        st.subheader("Rapport HTML")
        components.html(report_html, height=900, scrolling=True)
        st.download_button(
            "Telecharger le rapport HTML",
            data=report_html.encode("utf-8"),
            file_name="v2g_debug_report.html",
            mime="text/html",
        )
        st.download_button(
            "Telecharger le rapport PDF",
            data=report_pdf,
            file_name="v2g_debug_report.pdf",
            mime="application/pdf",
        )
        st.download_button(
            "Telecharger la timeline CSV",
            data=session_df.to_csv(index=False).encode("utf-8"),
            file_name="v2g_session_timeline.csv",
            mime="text/csv",
        )


def main() -> int:
    try:
        run_streamlit_app()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
