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
from reports.report_generator import generate_html_report
from timeline.reconstructor import build_timeseries_view
from utils.file_detector import detect_session_files
from utils.zip_loader import extract_zip_to_temp


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


def run_streamlit_app() -> None:
    try:
        import streamlit as st
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Streamlit n'est pas installé. Exécutez `pip install -r requirements.txt` "
            "puis lancez `streamlit run app/main.py`."
        ) from exc

    st.set_page_config(page_title="V2G Session Debugger", layout="wide")

    st.title("V2G Session Debugger")
    st.caption("Analyse automatique logs / PCAP / mesures pour debug borne vs véhicule")
    st.info("Workflow: Import session → détection fichiers → timeline → graphes → diagnostic → rapport")

    if "analysis" not in st.session_state:
        st.session_state.analysis = None

    with st.sidebar:
        st.header("Entrée session")
        input_mode = st.radio("Type de source", ["Fichier ZIP", "Dossier local"], index=0)
        folder_path = ""
        uploaded_zip = None

        if input_mode == "Dossier local":
            folder_path = st.text_input("Chemin dossier session", value="", placeholder="/path/to/session")
        else:
            uploaded_zip = st.file_uploader("Session ZIP", type=["zip"])

        analyze_clicked = st.button("Analyser", type="primary", use_container_width=True)

        if st.session_state.analysis is not None:
            dsum = st.session_state.analysis["detected_summary"]
            st.markdown("### Fichiers détectés (compact)")
            st.write(f"- EnergyManager: **{len(dsum.get('energy_manager', []))}**")
            st.write(f"- ChargerApp: **{len(dsum.get('charger_app', []))}**")
            st.write(f"- iotc-meter-dispatcher: **{len(dsum.get('iotc_meter_dispatcher', []))}**")
            st.write(f"- PCAP total: **{len(dsum.get('netlogger_pcaps', [])) + len(dsum.get('generic_pcaps', []))}**")
            st.write(f"  - netlogger PCAP: **{len(dsum.get('netlogger_pcaps', []))}**")
            st.write(f"  - pcap/pcaps PCAP: **{len(dsum.get('generic_pcaps', []))}**")
            st.write(f"- mesures CSV: **{len(dsum.get('dewesoft_csv', []))}**")
            st.write(f"- mesures d7d/dxd: **{len(dsum.get('dewesoft_raw', []))}** (conversion requise)")

            with st.expander("Voir les fichiers détectés"):
                st.json(
                    {
                        "energy_manager": dsum.get("energy_manager", []),
                        "charger_app": dsum.get("charger_app", []),
                        "iotc_meter_dispatcher": dsum.get("iotc_meter_dispatcher", []),
                        "netlogger_pcaps": dsum.get("netlogger_pcaps", []),
                        "netlogger_logs": dsum.get("netlogger_logs", []),
                        "dewesoft_csv": dsum.get("dewesoft_csv", []),
                        "dewesoft_raw": dsum.get("dewesoft_raw", []),
                        "generic_logs": dsum.get("generic_logs", []),
                        "generic_pcaps": dsum.get("generic_pcaps", []),
                    }
                )

    if analyze_clicked:
        temp_dir: tempfile.TemporaryDirectory[str] | None = None
        try:
            session_dir, temp_dir = _resolve_input_source(input_mode, folder_path, uploaded_zip)
            detected = detect_session_files(session_dir)
            session_df = build_session_timeline(detected)
            timeseries = build_timeseries_view(session_df)
            summary_lines = summarize_session(session_df)
            diagnostic = run_diagnostic(session_df)
            detected_summary = detected.to_summary()

            st.session_state.analysis = {
                "session_df": session_df,
                "timeseries": timeseries,
                "summary_lines": summary_lines,
                "diagnostic": diagnostic,
                "detected_summary": detected_summary,
            }
        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))
            st.exception(exc)
            st.session_state.analysis = None
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()

    analysis = st.session_state.analysis
    if analysis is None:
        st.warning("Aucune analyse disponible. Chargez une session puis cliquez sur Analyser.")
        return

    session_df: pd.DataFrame = analysis["session_df"]
    timeseries: pd.DataFrame = analysis["timeseries"]
    summary_lines: list[str] = analysis["summary_lines"]
    diagnostic: dict = analysis["diagnostic"]
    detected_summary: dict = analysis["detected_summary"]
    metrics = _compute_overview_metrics(session_df, detected_summary)

    tabs = st.tabs(["Vue d’ensemble", "Timeline", "Graphes", "Anomalies", "Diagnostic", "Rapport"])

    with tabs[0]:
        c1, c2, c3, c4 = st.columns(4)
        c5, c6, c7, c8 = st.columns(4)
        c1.metric("Fichiers analysés", metrics["files_analyzed"])
        c2.metric("Événements", metrics["events"])
        c3.metric("Erreurs", metrics["errors"])
        c4.metric("Warnings", metrics["warnings"])
        c5.metric("GridCodes", metrics["gridcodes"])
        c6.metric("Setpoints", metrics["setpoints"])
        c7.metric("PCAP détectés", metrics["pcaps"])
        c8.metric("Mesures détectées", metrics["measures"])

        st.markdown("### Résumé automatique")
        if session_df.empty:
            st.info("Aucun événement exploitable détecté dans la session.")
        else:
            readable_summary = (
                f"La session contient {metrics['events']} événements, avec {metrics['errors']} erreurs et "
                f"{metrics['warnings']} warnings. {metrics['setpoints']} changements de consigne ont été détectés, "
                f"ainsi que {metrics['gridcodes']} événements GridCodes. "
                f"Les sources disponibles incluent {len(detected_summary.get('energy_manager', []))} fichier(s) EnergyManager, "
                f"{len(detected_summary.get('charger_app', []))} fichier(s) ChargerApp et "
                f"{len(detected_summary.get('iotc_meter_dispatcher', []))} fichier(s) meter dispatcher."
            )
            st.write(readable_summary)
            st.markdown("**Résumé exécutif**")
            st.info(
                f"Cause probable: {diagnostic.get('cause_probable', 'indéterminé')} | "
                f"Confiance: {diagnostic.get('confidence_score', 0)}% | "
                f"{diagnostic.get('justification', '')}"
            )

    with tabs[1]:
        st.markdown("### Timeline filtrable")
        if session_df.empty:
            st.info("Timeline vide.")
        else:
            sources = sorted(session_df["source"].dropna().unique().tolist()) if "source" in session_df.columns else []
            event_types = sorted(session_df["event_type"].dropna().unique().tolist()) if "event_type" in session_df.columns else []

            col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
            source_filter = col_f1.multiselect("Source", sources)
            event_filter = col_f2.multiselect("Event type", event_types)
            text_query = col_f3.text_input("Recherche texte", value="")

            filtered = session_df.copy()
            if source_filter:
                filtered = filtered[filtered["source"].isin(source_filter)]
            if event_filter:
                filtered = filtered[filtered["event_type"].isin(event_filter)]
            if text_query:
                filtered = filtered[filtered["message"].astype(str).str.contains(text_query, case=False, na=False)]

            visible_columns = [c for c in ["timestamp", "source", "event_type", "message", "interpretation"] if c in filtered.columns]
            st.dataframe(filtered[visible_columns], use_container_width=True)

    with tabs[2]:
        st.markdown("### Graphes physiques")
        has_measure = not timeseries.empty and any(
            col in timeseries.columns and pd.to_numeric(timeseries[col], errors="coerce").notna().any()
            for col in ["P", "Q", "S", "U", "U_avg", "frequency", "frequency_Hz"]
        )
        if has_measure:
            st.plotly_chart(build_signal_figure(timeseries), use_container_width=True)
        else:
            st.info(
                "Aucune mesure exploitable détectée. Importer un export CSV Dewesoft "
                "pour activer les graphes physiques."
            )

    with tabs[3]:
        st.markdown("### Anomalies détectées")
        if session_df.empty:
            st.info("Aucune anomalie: timeline vide.")
        else:
            anomaly_types = ["error", "warning", "power_limit", "timeout", "protocol_event", "gridcodes"]
            anomalies = session_df[session_df["event_type"].isin(anomaly_types)].copy()
            if anomalies.empty:
                st.success("Aucune anomalie majeure détectée dans les catégories surveillées.")
            else:
                st.dataframe(
                    anomalies[[c for c in ["timestamp", "source", "event_type", "message"] if c in anomalies.columns]],
                    use_container_width=True,
                )

    with tabs[4]:
        st.markdown("### Conclusion diagnostic")
        conclusion = diagnostic.get("cause_probable", "indéterminé")
        confidence = diagnostic.get("confidence_score", 0)

        st.success(f"Origine probable : **{conclusion}**")
        st.write(f"**Niveau de confiance :** {confidence}%")

        st.markdown("**Explication claire**")
        st.write(diagnostic.get("justification", "Aucune justification disponible."))

        st.markdown("**Preuves utilisées**")
        for ev in diagnostic.get("evidence", []):
            st.write(f"- {ev}")

        st.markdown("**Données manquantes**")
        missing_data = diagnostic.get("missing_data", [])
        st.write(", ".join(missing_data) if missing_data else "Aucune donnée critique manquante détectée.")

        csv_count = len(detected_summary.get("dewesoft_csv", []))
        raw_count = len(detected_summary.get("dewesoft_raw", []))
        st.markdown("**Données Dewesoft**")
        if csv_count > 0:
            st.write(f"Mesures CSV disponibles: {csv_count}")
        elif raw_count > 0:
            st.warning(f"{raw_count} fichier(s) Dewesoft .d7d/.dxd détecté(s): conversion Dewesoft requise.")
        else:
            st.info("Aucune acquisition Dewesoft détectée.")

    with tabs[5]:
        st.markdown("### Export rapport")
        report_html = generate_html_report(summary_lines, diagnostic, session_df, detected_summary)
        st.download_button(
            "Télécharger rapport HTML",
            data=report_html.encode("utf-8"),
            file_name="v2g_debug_report.html",
            mime="text/html",
        )
        st.download_button(
            "Télécharger timeline (CSV)",
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
