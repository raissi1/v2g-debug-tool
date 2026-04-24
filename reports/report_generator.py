"""Simple HTML report generation for V2G generic debug."""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def generate_html_report(summary_lines: list[str], diagnostic: dict, timeline: pd.DataFrame) -> str:
    issues_html = "".join(f"<li>{issue}</li>" for issue in diagnostic.get("issues", []))
    summary_html = "".join(f"<li>{line}</li>" for line in summary_lines)

    table_html = "<p>Aucune donnée timeline.</p>"
    if not timeline.empty:
        preview = timeline.head(200)
        table_html = preview.to_html(index=False, escape=True)

    return f"""
    <html>
      <head><meta charset='utf-8'><title>V2G Debug Report</title></head>
      <body>
        <h1>Rapport de debug V2G</h1>
        <p>Généré le: {datetime.utcnow().isoformat()}Z</p>

        <h2>Résumé</h2>
        <ul>{summary_html}</ul>

        <h2>Anomalies détectées</h2>
        <ul>{issues_html}</ul>

        <h2>Conclusion</h2>
        <p><strong>{diagnostic.get('conclusion', 'Indéterminé')}</strong></p>

        <h2>Timeline (aperçu)</h2>
        {table_html}
      </body>
    </html>
    """
