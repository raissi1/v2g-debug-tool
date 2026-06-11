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


def _inject_premium_css(st) -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(14, 116, 144, 0.10), transparent 30%),
                radial-gradient(circle at top right, rgba(2, 132, 199, 0.08), transparent 28%),
                linear-gradient(180deg, #f5f8fc 0%, #edf3f9 100%);
        }
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f8fbff 0%, #eef4fb 100%);
            border-right: 1px solid rgba(15, 23, 42, 0.08);
        }
        .premium-hero {
            padding: 28px 30px;
            border-radius: 28px;
            color: white;
            background: linear-gradient(135deg, #0f766e 0%, #155e75 45%, #0f172a 100%);
            box-shadow: 0 20px 60px rgba(15, 23, 42, 0.16);
            margin-bottom: 18px;
        }
        .premium-hero h1 {
            margin: 0;
            font-size: 3.1rem;
            line-height: 1.02;
            letter-spacing: -0.03em;
        }
        .premium-hero p {
            margin: 10px 0 0 0;
            font-size: 1.05rem;
            color: rgba(255,255,255,0.92);
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
            background: linear-gradient(180deg, #fefce8 0%, #fff7cc 100%);
            border: 1px solid rgba(202, 138, 4, 0.16);
            border-radius: 18px;
            padding: 14px 16px;
            color: #854d0e;
            margin: 10px 0 16px 0;
        }
        div[data-testid="stMetric"] {
            background: rgba(255,255,255,0.88);
            border: 1px solid rgba(15,23,42,0.06);
            padding: 14px 16px;
            border-radius: 18px;
            box-shadow: 0 8px 24px rgba(15,23,42,0.05);
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

    st.markdown(
        """
        <div class="premium-hero">
          <h1>V2G Session Debugger</h1>
          <p>Analyse multi-sources premium pour reconstituer la session, localiser l'ecart et produire un rapport exploitable en HTML et PDF.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="premium-note">
          Workflow metier: importer une session avec logs, PCAP, captures et mesures Dewesoft,
          reconstruire la timeline, correler les sources puis formuler un verdict motive.
        </div>
        """,
        unsafe_allow_html=True,
    )

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
        st.warning("Aucune analyse disponible. Chargez une session puis cliquez sur Analyser la session.")
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
