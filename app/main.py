"""Streamlit entrypoint for the V2G debug tool."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from analyzers.generic_debug import summarize_session
from core.session_builder import build_session_timeline
from utils.file_detector import detect_session_files
from utils.zip_loader import extract_zip_to_temp


st.set_page_config(page_title="V2G Debug Tool", layout="wide")
st.title("V2G Session Debugger")
st.caption(
    "Chargez un dossier de session ou un ZIP pour reconstruire la chronologie et accélérer le diagnostic."
)


with st.sidebar:
    st.header("Entrée")
    input_mode = st.radio("Type de source", ["Dossier local", "Fichier ZIP"], index=1)

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
    work_dir: Path | None = None
    temp_dir: tempfile.TemporaryDirectory[str] | None = None

    try:
        if input_mode == "Dossier local":
            if not folder_path:
                st.error("Veuillez indiquer un dossier.")
                st.stop()
            work_dir = Path(folder_path).expanduser().resolve()
            if not work_dir.is_dir():
                st.error(f"Le dossier n'existe pas: {work_dir}")
                st.stop()
        else:
            if uploaded_zip is None:
                st.error("Veuillez charger un fichier ZIP.")
                st.stop()
            temp_dir = tempfile.TemporaryDirectory(prefix="v2g_session_")
            zip_path = Path(temp_dir.name) / uploaded_zip.name
            with zip_path.open("wb") as out:
                out.write(uploaded_zip.getvalue())
            work_dir = extract_zip_to_temp(zip_path, Path(temp_dir.name))

        detected = detect_session_files(work_dir)
        session_df = build_session_timeline(detected)

        st.subheader("Fichiers détectés")
        st.json(detected.to_summary())

        st.subheader("Analyse")
        summary = summarize_session(session_df)
        for line in summary:
            st.write(f"- {line}")

        st.subheader("Timeline")
        if session_df.empty:
            st.info("Aucun événement parsé.")
        else:
            st.dataframe(session_df, use_container_width=True)

            csv_bytes = session_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Télécharger la timeline (CSV)",
                data=csv_bytes,
                file_name="v2g_session_timeline.csv",
                mime="text/csv",
            )

            with st.expander("Statistiques rapides"):
                st.write("Événements par source")
                counts = session_df["source"].value_counts().rename_axis("source").reset_index(name="count")
                st.dataframe(counts, use_container_width=True)

                if "timestamp" in session_df.columns:
                    ts = pd.to_datetime(session_df["timestamp"], utc=True, errors="coerce").dropna()
                    if not ts.empty:
                        duration = ts.max() - ts.min()
                        st.metric("Durée couverte", str(duration))

    except Exception as exc:  # noqa: BLE001
        st.exception(exc)
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


st.markdown("---")
st.caption("Python 3.11 • Streamlit • pandas • Architecture modulaire")
