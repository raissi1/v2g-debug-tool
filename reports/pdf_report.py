"""PDF report generation for the V2G debug workflow."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


CAUSE_LABELS = {
    "borne": "Cote borne",
    "vehicule": "Cote vehicule",
    "communication": "Cote communication",
    "indetermine": "Indetermine",
}


def _label_cause(value: str | None) -> str:
    return CAUSE_LABELS.get(str(value or "").lower(), str(value or "Indetermine"))


def _styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CoverTitle", fontName="Helvetica-Bold", fontSize=24, leading=30, textColor=colors.HexColor("#0f172a"), spaceAfter=12))
    styles.add(ParagraphStyle(name="SectionTitle", fontName="Helvetica-Bold", fontSize=16, leading=20, textColor=colors.HexColor("#0f172a"), spaceBefore=8, spaceAfter=8))
    styles.add(ParagraphStyle(name="SubTitle", fontName="Helvetica-Bold", fontSize=12, leading=15, textColor=colors.HexColor("#164e63"), spaceBefore=6, spaceAfter=6))
    styles.add(ParagraphStyle(name="BodySmall", parent=styles["BodyText"], fontSize=9.5, leading=13))
    return styles


def _bullet_paragraphs(values: list[str], style: ParagraphStyle, empty_label: str = "Aucun element exploitable") -> list:
    entries = values or [empty_label]
    return [Paragraph(f"• {str(entry)}", style) for entry in entries]


def _frame_to_table(frame: pd.DataFrame, columns: list[str], empty_label: str) -> Table:
    if frame.empty:
        return Table([[empty_label]], colWidths=[170 * mm])
    keep = [column for column in columns if column in frame.columns]
    if not keep:
        return Table([[empty_label]], colWidths=[170 * mm])

    preview = frame[keep].head(20).fillna("")
    header = [str(column) for column in preview.columns]
    rows = [[str(value)[:120] for value in row] for row in preview.to_numpy().tolist()]
    table = Table([header, *rows], repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("LEADING", (0, 0), (-1, -1), 10),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ]
        )
    )
    return table


def generate_pdf_report(
    summary_lines: list[str],
    diagnostic: dict,
    timeline: pd.DataFrame,
    detected_summary: dict | None = None,
) -> bytes:
    styles = _styles()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm, topMargin=16 * mm, bottomMargin=16 * mm)
    story = []

    cause = diagnostic.get("cause_probable", "indetermine")
    confidence = diagnostic.get("confidence", "Faible")
    confidence_score = diagnostic.get("confidence_score", 0)
    issue_origin = diagnostic.get("issue_origin", {}) or {}
    first_divergence = diagnostic.get("first_divergence", {}) or {}
    cross = diagnostic.get("cross_analysis", {}) or {}
    generic_rules = diagnostic.get("generic_rules", []) or []
    generic_rule_summary = diagnostic.get("generic_rule_summary", {}) or {}

    story.append(Paragraph("Rapport d'analyse V2G", styles["CoverTitle"]))
    story.append(Paragraph(f"Verdict principal: <b>{_label_cause(cause)}</b>", styles["BodyText"]))
    story.append(Paragraph(f"Confiance: {confidence} ({confidence_score}%)", styles["BodyText"]))
    story.append(Paragraph(diagnostic.get("executive_summary", ""), styles["BodyText"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Synthese", styles["SectionTitle"]))
    story.extend(_bullet_paragraphs(summary_lines, styles["BodySmall"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Verdict et point de depart probable", styles["SectionTitle"]))
    story.append(Paragraph(diagnostic.get("justification", "Aucune justification disponible."), styles["BodyText"]))
    origin_text = issue_origin.get("reason", "Point de depart du probleme non determine.")
    if issue_origin.get("timestamp") or issue_origin.get("source"):
        origin_text = f"{issue_origin.get('timestamp')} - {issue_origin.get('source')} - {origin_text}"
    story.append(Paragraph(origin_text, styles["BodyText"]))
    story.append(Paragraph(f"Piste principale: {_label_cause(diagnostic.get('best_lead', 'indetermine'))}", styles["BodyText"]))
    story.append(Paragraph(diagnostic.get("best_lead_reason", ""), styles["BodyText"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph("Premier point de divergence", styles["SubTitle"]))
    divergence_text = first_divergence.get("reason", "Aucun point de divergence net n'a ete determine.")
    if first_divergence.get("timestamp") or first_divergence.get("source"):
        divergence_text = (
            f"{first_divergence.get('timestamp')} - {first_divergence.get('source')} - "
            f"{first_divergence.get('category')} - {divergence_text}"
        )
    story.append(Paragraph(divergence_text, styles["BodyText"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Sources detectees", styles["SectionTitle"]))
    if detected_summary:
        coverage = detected_summary.get("coverage", {})
        dewesoft_coverage = coverage.get("dewesoft", {})
        file_lines = [
            f"EnergyManager: {len(detected_summary.get('energy_manager', []))} fichier(s)",
            f"ChargerApp: {len(detected_summary.get('charger_app', []))} fichier(s)",
            f"Meter dispatcher: {len(detected_summary.get('iotc_meter_dispatcher', []))} fichier(s)",
            f"PCAP: {len(detected_summary.get('netlogger_pcaps', [])) + len(detected_summary.get('generic_pcaps', []))} fichier(s)",
            f"Dewesoft CSV: {len(detected_summary.get('dewesoft_csv', []))} fichier(s)",
            f"Dewesoft brut (.d7d/.dxd/.dmd): {len(detected_summary.get('dewesoft_raw', []))} fichier(s)",
            f"Captures / images: {len(detected_summary.get('supporting_images', []))} fichier(s)",
        ]
        if dewesoft_coverage:
            file_lines.append(
                "Statut Dewesoft: "
                f"{dewesoft_coverage.get('csv_ready', 0)} CSV prets, "
                f"{dewesoft_coverage.get('sidecar_csv', 0)} bruts associes a un CSV, "
                f"{dewesoft_coverage.get('conversion_required', 0)} conversions requises"
            )
        story.extend(_bullet_paragraphs(file_lines, styles["BodySmall"]))
    else:
        story.append(Paragraph("Detection des fichiers non fournie.", styles["BodyText"]))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Raisonnement detaille", styles["SectionTitle"]))
    blocks = diagnostic.get("blocks", {})
    section_map = [
        ("1. Ce qui a ete demande", blocks.get("A_requested", [])),
        ("2. Ce que la borne a calcule ou publie", blocks.get("B_station_computed", [])),
        ("3. Ce qui a ete envoye au vehicule", blocks.get("C_sent_to_vehicle", [])),
        ("4. Ce qui a ete mesure", blocks.get("D_measured", [])),
        ("5. Anomalies retenues", blocks.get("E_anomalies", [])),
    ]
    for title, lines in section_map:
        story.append(Paragraph(title, styles["SubTitle"]))
        story.extend(_bullet_paragraphs(lines, styles["BodySmall"]))
        story.append(Spacer(1, 4))

    story.append(PageBreak())
    story.append(Paragraph("Ecarts et preuves", styles["SectionTitle"]))
    story.append(Paragraph("Indices de correlation inter-sources", styles["SubTitle"]))
    story.extend(_bullet_paragraphs(cross.get("insights", []), styles["BodySmall"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Preuves fortes", styles["SubTitle"]))
    story.extend(_bullet_paragraphs(diagnostic.get("evidence", []), styles["BodySmall"]))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Donnees manquantes", styles["SubTitle"]))
    story.extend(_bullet_paragraphs(diagnostic.get("missing_data", []), styles["BodySmall"], empty_label="Aucune donnee critique manquante detectee"))
    story.append(Spacer(1, 10))

    story.append(Paragraph("Regles d'analyse generiques", styles["SectionTitle"]))
    story.append(
        Paragraph(
            f"Pass={generic_rule_summary.get('pass', 0)} | Warn={generic_rule_summary.get('warn', 0)} | "
            f"Fail={generic_rule_summary.get('fail', 0)} | Unknown={generic_rule_summary.get('unknown', 0)}",
            styles["BodyText"],
        )
    )
    story.append(
        _frame_to_table(
            pd.DataFrame(generic_rules),
            ["title", "category", "status", "severity", "expected", "observed", "reason", "timestamp", "source"],
            "Aucune regle generique evaluee.",
        )
    )
    story.append(Spacer(1, 10))

    story.append(Paragraph("Table comparative", styles["SectionTitle"]))
    story.append(
        _frame_to_table(
            pd.DataFrame(cross.get("rows", [])),
            [
                "timestamp",
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
                "message",
            ],
            "Aucune table comparative disponible.",
        )
    )
    story.append(Spacer(1, 10))
    story.append(Paragraph("Preuves structurees", styles["SectionTitle"]))
    story.append(
        _frame_to_table(
            pd.DataFrame(diagnostic.get("evidence_table", [])),
            ["timestamp", "source", "type", "impact", "weight", "comment"],
            "Aucune preuve structuree.",
        )
    )
    story.append(Spacer(1, 10))
    story.append(Paragraph("Timeline de reference", styles["SectionTitle"]))
    story.append(
        _frame_to_table(
            timeline,
            ["timestamp", "source", "event_type", "message"],
            "Aucune timeline exploitable.",
        )
    )

    if detected_summary and detected_summary.get("supporting_images", []):
        story.append(PageBreak())
        story.append(Paragraph("Captures et illustrations", styles["SectionTitle"]))
        story.append(Paragraph("Extraits visuels detectes dans le package de session.", styles["BodyText"]))
        for raw_path in detected_summary.get("supporting_images", [])[:6]:
            path = Path(raw_path)
            if not path.exists():
                continue
            try:
                story.append(Paragraph(path.name, styles["SubTitle"]))
                image = Image(str(path))
                image._restrictSize(170 * mm, 90 * mm)
                story.append(image)
                story.append(Spacer(1, 8))
            except Exception:
                continue

    doc.build(story)
    return buffer.getvalue()
