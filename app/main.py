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

from analyzers.generic_debug import summarize_session
from core.session_builder import build_session_timeline
from diagnostics.generic_diagnostic import run_generic_diagnostic
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


def run_streamlit_app() -> None:
    try:
        import streamlit as st
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Streamlit n'est pas installé. Exécutez `pip install -r requirements.txt` "
            "puis lancez `streamlit run app/main.py`."
        ) from exc

    st.set_page_config(page_title="V2G Debug Tool", layout="wide")
    st.title("V2G Session Debugger - complet")
    st.caption("Pipeline modulaire: ingestion, parsing, timeline, graphes, diagnostic et rapport.")

    with st.sidebar:
        st.header("Entrée")
        input_mode = st.radio("Type de source", ["Fichier ZIP", "Dossier local"], index=0)
        folder_path = ""
        uploaded_zip = None
        if input_mode == "Dossier local":
            folder_path = st.text_input("Chemin du dossier", value="", placeholder="/path/to/session")
        else:
            uploaded_zip = st.file_uploader("Session ZIP", type=["zip"])

    if st.button("Analyser", type="primary"):
        temp_dir: tempfile.TemporaryDirectory[str] | None = None
        try:
            session_dir, temp_dir = _resolve_input_source(input_mode, folder_path, uploaded_zip)
            detected = detect_session_files(session_dir)
            session_df = build_session_timeline(detected)
            timeseries = build_timeseries_view(session_df)
            summary_lines = summarize_session(session_df)
            diagnostic = run_generic_diagnostic(session_df)

            st.subheader("Résumé des fichiers détectés")
            summary = detected.to_summary()
            st.json(
                {
                    "root": summary.get("root"),
                    "aux_root": summary.get("aux_root"),
                    "charger_app": summary.get("charger_app", []),
                    "energy_manager": summary.get("energy_manager", []),
                    "iotc_meter_dispatcher": summary.get("iotc_meter_dispatcher", []),
                    "netlogger_pcaps": summary.get("netlogger_pcaps", []),
                    "netlogger_logs": summary.get("netlogger_logs", []),
                    "dewesoft_csv": summary.get("dewesoft_csv", []),
                }
            )
            st.caption(f"Fichiers ignorés: {len(summary.get('ignored_files', []))}")

            st.subheader("Timeline")
            st.dataframe(session_df, use_container_width=True)

            st.subheader("Graphes P/Q/U/fréquence")
            st.plotly_chart(build_signal_figure(timeseries), use_container_width=True)

            st.subheader("Analyse debug")
            for line in summary_lines:
                st.write(f"- {line}")
            st.markdown("**Conclusion générique**")
            st.write(diagnostic.get("conclusion", "Indéterminé"))
            st.markdown("**Anomalies**")
            for issue in diagnostic.get("issues", []):
                st.write(f"- {issue}")

            st.subheader("Export rapport")
            report_html = generate_html_report(summary_lines, diagnostic, session_df)
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

        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))
            st.exception(exc)
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()

    st.markdown("---")
    st.caption("Scope logs/pcap: /var/aux ; mesures Dewesoft CSV support (d7d/dxd via extension future).")


def main() -> int:
    try:
        run_streamlit_app()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
