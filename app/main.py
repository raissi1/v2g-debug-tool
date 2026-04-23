"""Streamlit entrypoint for the V2G debug tool.

Usage:
    streamlit run app/main.py

If Streamlit is missing, this module prints a clear installation hint when
executed directly with `python app/main.py`.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any


def _bootstrap_import_paths() -> None:
    """Ensure repository-local packages are importable in all execution modes.

    This handles cases like:
      - `streamlit run app/main.py`
      - `python app/main.py`
      - launching from another current working directory
    """
    script_path = Path(__file__).resolve()
    candidates = [
        script_path.parents[1],  # repo root when script is app/main.py
        Path.cwd().resolve(),    # current working directory
        script_path.parent,      # app/
    ]

    for candidate in candidates:
        if not candidate.exists():
            continue
        if (candidate / "analyzers").exists() and str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))


_bootstrap_import_paths()

import pandas as pd

from analyzers.generic_debug import summarize_session
from core.session_builder import build_session_timeline
from utils.file_detector import detect_session_files
from utils.zip_loader import extract_zip_to_temp


def _resolve_input_source(
    input_mode: str,
    folder_path: str,
    uploaded_zip: Any,
) -> tuple[Path, tempfile.TemporaryDirectory[str] | None]:
    """Return the session directory to analyze and optional temp directory."""
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
    """Run the Streamlit UI."""
    try:
        import streamlit as st
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Streamlit n'est pas installé. Exécutez `pip install -r requirements.txt` "
            "puis lancez `streamlit run app/main.py`."
        ) from exc

    st.set_page_config(page_title="V2G Debug Tool", layout="wide")
    st.title("V2G Session Debugger")
    st.caption("Analyse d'une session à partir d'un dossier local ou d'un fichier ZIP.")

    with st.sidebar:
        st.header("Entrée")
        input_mode = st.radio("Type de source", ["Fichier ZIP", "Dossier local"], index=0)

        folder_path = ""
        uploaded_zip = None

        if input_mode == "Dossier local":
            folder_path = st.text_input(
                "Chemin du dossier",
                value="",
                placeholder="/path/to/session",
                help="Chemin sur la machine qui exécute Streamlit.",
            )
        else:
            uploaded_zip = st.file_uploader("Session ZIP", type=["zip"])

    if st.button("Analyser", type="primary"):
        temp_dir: tempfile.TemporaryDirectory[str] | None = None
        try:
            session_dir, temp_dir = _resolve_input_source(input_mode, folder_path, uploaded_zip)

            detected = detect_session_files(session_dir)
            session_df = build_session_timeline(detected)

            st.subheader("Fichiers détectés")
            st.json(detected.to_summary())

            st.subheader("Analyse")
            for line in summarize_session(session_df):
                st.write(f"- {line}")

            st.subheader("Timeline")
            if session_df.empty:
                st.info("Aucun événement parsé.")
            else:
                st.dataframe(session_df, use_container_width=True)

                st.download_button(
                    "Télécharger la timeline (CSV)",
                    data=session_df.to_csv(index=False).encode("utf-8"),
                    file_name="v2g_session_timeline.csv",
                    mime="text/csv",
                )

                with st.expander("Statistiques rapides"):
                    counts = session_df["source"].value_counts().rename_axis("source").reset_index(name="count")
                    st.dataframe(counts, use_container_width=True)

                    ts = pd.to_datetime(session_df.get("timestamp"), utc=True, errors="coerce").dropna()
                    if not ts.empty:
                        st.metric("Durée couverte", str(ts.max() - ts.min()))

        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))
            st.exception(exc)
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()

    st.markdown("---")
    st.caption("Détection automatique: EnergyManager • ChargerApp • iotc-meter-dispatcher • PCAP • mesures")


def main() -> int:
    """CLI fallback when executed directly via Python."""
    try:
        run_streamlit_app()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
