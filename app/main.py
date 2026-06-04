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


def _render_detected_files(st, detected_summary: dict) -> None:
    pcap_total = len(detected_summary.get("netlogger_pcaps", [])) + len(detected_summary.get("generic_pcaps", []))
    st.write(f"EnergyManager: **{len(detected_summary.get('energy_manager', []))}**")
    st.write(f"ChargerApp: **{len(detected_summary.get('charger_app', []))}**")
    st.write(f"iotc-meter-dispatcher: **{len(detected_summary.get('iotc_meter_dispatcher', []))}**")
    st.write(f"PCAP detectes: **{pcap_total}**")
    st.write(f"Mesures Dewesoft CSV: **{len(detected_summary.get('dewesoft_csv', []))}**")
    st.write(f"Mesures Dewesoft brutes (.d7d/.dxd): **{len(detected_summary.get('dewesoft_raw', []))}**")


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

    st.title("V2G Session Debugger")
    st.caption("Analyse automatique d'une session ZIP ou d'un dossier local pour trancher entre borne, vehicule et communication.")
    st.info(
        "Workflow metier: importer une session avec logs, PCAP et mesures Dewesoft, "
        "reconstruire la timeline, correler les sources puis generer le verdict."
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

        st.caption("Le ZIP peut contenir les logs borne, les traces PCAP et les exports Dewesoft CSV.")
        analyze_clicked = st.button("Analyser la session", type="primary", use_container_width=True)

        if st.session_state.analysis is not None:
            st.markdown("### Sources detectees")
            _render_detected_files(st, st.session_state.analysis["detected_summary"])
            with st.expander("Voir le detail des fichiers detectes"):
                st.json(st.session_state.analysis["detected_summary"])

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
        st.warning("Aucune analyse disponible. Chargez une session puis cliquez sur Analyser la session.")
        return

    session_df: pd.DataFrame = analysis["session_df"]
    timeseries: pd.DataFrame = analysis["timeseries"]
    summary_lines: list[str] = analysis["summary_lines"]
    diagnostic: dict = analysis["diagnostic"]
    detected_summary: dict = analysis["detected_summary"]
    metrics = _compute_overview_metrics(session_df, detected_summary)
    report_html = generate_html_report(summary_lines, diagnostic, session_df, detected_summary)

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
        _render_detected_files(st, detected_summary)

        st.markdown("### Detail des types de donnees")
        st.write("Logs borne: utilises pour reconstruire la consigne, les recalculs, les limitations et les erreurs.")
        st.write("PCAP: utilises pour confirmer les echanges protocole quand ils sont exploitables dans la timeline.")
        st.write("Dewesoft CSV: utilises pour comparer les mesures physiques reelles avec les consignes et le meter interne.")

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
            st.dataframe(filtered[visible_columns], use_container_width=True)

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
            st.plotly_chart(build_signal_figure(timeseries), use_container_width=True)
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
                use_container_width=True,
            )

        st.markdown("### Dewesoft")
        csv_count = len(detected_summary.get("dewesoft_csv", []))
        raw_count = len(detected_summary.get("dewesoft_raw", []))
        if csv_count > 0:
            st.write(f"Mesures CSV disponibles: {csv_count}")
        elif raw_count > 0:
            st.warning(f"{raw_count} fichier(s) Dewesoft .d7d/.dxd detecte(s): conversion Dewesoft requise.")
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
