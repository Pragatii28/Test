"""
rca/pdf_report.py — PDF Report Generator for RCA Engine
Uses reportlab to produce professional, formatted RCA PDF reports.

Usage:
    from rca.engine import RCAEngine
    from rca.pdf_report import generate_pdf

    engine = RCAEngine("observability_data/metrics.db")
    report = engine.run_rca("i-0abc123", "cpu_utilization_percent")
    pdf_path = generate_pdf(report, output_dir="rca_reports")
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")   # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate,
    Paragraph, Spacer, Table, TableStyle,
    Image, HRFlowable, KeepTogether, PageBreak,
)
from reportlab.platypus.flowables import BalancedColumns

# ── Import our engine types ────────────────────────────────────────────────────
from rca.engine import RCAReport, CausalCandidate, ErrorLogEntry


# ── Colour palette ─────────────────────────────────────────────────────────────
C_DARK   = colors.HexColor("#1A1F2E")
C_RED    = colors.HexColor("#E53935")
C_ORANGE = colors.HexColor("#FB8C00")
C_GREEN  = colors.HexColor("#43A047")
C_BLUE   = colors.HexColor("#1565C0")
C_LIGHT  = colors.HexColor("#F5F7FA")
C_BORDER = colors.HexColor("#CBD2DB")
C_MUTED  = colors.HexColor("#6B7280")
C_WHITE  = colors.white

PAGE_W, PAGE_H = A4
MARGIN = 2 * cm


# ── Style sheet ────────────────────────────────────────────────────────────────

def _styles():
    ss = getSampleStyleSheet()

    def add(name, **kw):
        ss.add(ParagraphStyle(name=name, **kw))

    add("RCA_Title",
        fontName="Helvetica-Bold", fontSize=22, textColor=C_WHITE,
        spaceAfter=4, leading=28)
    add("RCA_Subtitle",
        fontName="Helvetica", fontSize=11, textColor=colors.HexColor("#B0BEC5"),
        spaceAfter=2, leading=15)
    add("RCA_H1",
        fontName="Helvetica-Bold", fontSize=13, textColor=C_DARK,
        spaceBefore=14, spaceAfter=6, leading=17)
    add("RCA_H2",
        fontName="Helvetica-Bold", fontSize=11, textColor=C_BLUE,
        spaceBefore=10, spaceAfter=4, leading=14)
    add("RCA_Body",
        fontName="Helvetica", fontSize=9, textColor=C_DARK,
        spaceAfter=4, leading=13)
    add("RCA_Small",
        fontName="Helvetica", fontSize=8, textColor=C_MUTED,
        spaceAfter=2, leading=11)
    add("RCA_Code",
        fontName="Courier", fontSize=8, textColor=C_DARK,
        spaceAfter=2, leading=11, backColor=C_LIGHT,
        leftIndent=6, rightIndent=6)
    add("RCA_Badge_Critical",
        fontName="Helvetica-Bold", fontSize=9, textColor=C_WHITE,
        backColor=C_RED, alignment=TA_CENTER, leading=12)
    add("RCA_Badge_Warning",
        fontName="Helvetica-Bold", fontSize=9, textColor=C_WHITE,
        backColor=C_ORANGE, alignment=TA_CENTER, leading=12)
    add("RCA_Center",
        fontName="Helvetica", fontSize=9, textColor=C_MUTED,
        alignment=TA_CENTER, leading=12)
    add("RCA_Action",
        fontName="Helvetica", fontSize=9, textColor=C_DARK,
        spaceAfter=3, leftIndent=12, leading=13)

    return ss


# ── Header / Footer ────────────────────────────────────────────────────────────

def _header_footer(canvas, doc):
    canvas.saveState()
    W = PAGE_W

    # Top banner
    canvas.setFillColor(C_DARK)
    canvas.rect(0, PAGE_H - 1.6 * cm, W, 1.6 * cm, fill=1, stroke=0)

    canvas.setFont("Helvetica-Bold", 10)
    canvas.setFillColor(C_WHITE)
    canvas.drawString(MARGIN, PAGE_H - 1.1 * cm, "RCA Report  |  Multi-Cloud Observability")

    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(colors.HexColor("#90A4AE"))
    canvas.drawRightString(W - MARGIN, PAGE_H - 1.1 * cm, doc.rca_report_id)

    # Bottom bar
    canvas.setFillColor(C_BORDER)
    canvas.rect(0, 0, W, 1.0 * cm, fill=1, stroke=0)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(C_MUTED)
    canvas.drawString(MARGIN, 0.35 * cm, f"Generated: {doc.generated_at}")
    canvas.drawRightString(W - MARGIN, 0.35 * cm, f"Page {doc.page}")

    canvas.restoreState()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sev_color(sev: str) -> colors.Color:
    return C_RED if sev == "critical" else C_ORANGE


def _confidence_bar(value: float, width: float = 8 * cm) -> Image:
    """Returns a tiny matplotlib bar rendered as a ReportLab Image."""
    fig, ax = plt.subplots(figsize=(4, 0.35))
    ax.barh(0, value, color="#1565C0", height=0.6)
    ax.barh(0, 1 - value, left=value, color="#E0E7EF", height=0.6)
    ax.set_xlim(0, 1)
    ax.axis("off")
    ax.text(value + 0.02, 0, f"{value*100:.0f}%",
            va="center", ha="left", fontsize=7, color="#1A1F2E")
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, transparent=True)
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=width, height=0.5 * cm)


def _metric_chart(
    history: List[Tuple[str, float]],
    label: str,
    width: float = 14 * cm,
    height: float = 4 * cm,
) -> Optional[Image]:
    if not history:
        return None
    try:
        times = [datetime.fromisoformat(t.replace("Z", "+00:00")) for t, _ in history]
        vals  = [v for _, v in history]

        fig, ax = plt.subplots(figsize=(width / cm, height / cm))
        ax.plot(times, vals, color="#1565C0", linewidth=1.4, zorder=3)
        ax.fill_between(times, vals, alpha=0.12, color="#1565C0")

        mean = np.mean(vals)
        ax.axhline(mean, color="#6B7280", linestyle="--", linewidth=0.8, label=f"avg {mean:.2f}")

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.tick_params(axis="both", labelsize=6)
        ax.set_title(label, fontsize=7, color="#1A1F2E", pad=3)
        ax.legend(fontsize=6, loc="upper right")
        ax.grid(True, alpha=0.25)
        fig.tight_layout(pad=0.5)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return Image(buf, width=width, height=height)
    except Exception as exc:
        print(f"[PDF] chart failed: {exc}")
        return None


def _table(data: List[List], col_widths: List[float], style_extra=None) -> Table:
    base_style = [
        ("BACKGROUND", (0, 0), (-1, 0), C_DARK),
        ("TEXTCOLOR",  (0, 0), (-1, 0), C_WHITE),
        ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",   (0, 0), (-1, 0), 8),
        ("FONTNAME",   (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",   (0, 1), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [C_WHITE, C_LIGHT]),
        ("GRID",       (0, 0), (-1, -1), 0.4, C_BORDER),
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]
    if style_extra:
        base_style.extend(style_extra)
    return Table(data, colWidths=col_widths, style=TableStyle(base_style), repeatRows=1)


# ── Section builders ───────────────────────────────────────────────────────────

def _cover_section(rpt: RCAReport, ss) -> List:
    """Dark cover block at top of page 1."""
    story = []

    # Cover banner (simulate with a coloured table)
    sev_color = _sev_color(rpt.trigger_severity)
    banner_data = [[
        Paragraph(f"Root Cause Analysis Report", ss["RCA_Title"]),
        Paragraph(rpt.report_id, ss["RCA_Subtitle"]),
    ]]
    banner_table = Table(
        [[Paragraph(f"<b>ROOT CAUSE ANALYSIS REPORT</b>", ss["RCA_Title"]),
          Paragraph(rpt.report_id, ss["RCA_Subtitle"])]],
        colWidths=[12 * cm, 5 * cm],
    )
    banner_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), C_DARK),
        ("TOPPADDING",    (0, 0), (-1, -1), 14),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(banner_table)
    story.append(Spacer(1, 0.3 * cm))

    # KPI row
    ts_fmt = rpt.trigger_anomaly_time[:19].replace("T", " ")
    gen_fmt = rpt.generated_at[:19].replace("T", " ")

    kpi = [
        ["Triggered", ts_fmt],
        ["Resource",  rpt.trigger_resource],
        ["Type",      rpt.trigger_resource_type],
        ["Metric",    rpt.trigger_metric],
        ["Value",     f"{rpt.trigger_value:.4f}"],
        ["Severity",  rpt.trigger_severity.upper()],
        ["Cloud",     rpt.trigger_cloud],
        ["Region",    rpt.trigger_region],
        ["Generated", gen_fmt],
    ]
    kpi_table = Table(
        [[Paragraph(f"<b>{k}</b>", ss["RCA_Small"]),
          Paragraph(str(v), ss["RCA_Body"])]
         for k, v in kpi],
        colWidths=[3.5 * cm, 13 * cm],
    )
    kpi_table.setStyle(TableStyle([
        ("GRID",       (0, 0), (-1, -1), 0.4, C_BORDER),
        ("BACKGROUND", (0, 0), (0, -1), C_LIGHT),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
        # Highlight severity row
        ("BACKGROUND", (1, 5), (1, 5), sev_color),
        ("TEXTCOLOR",  (1, 5), (1, 5), C_WHITE),
        ("FONTNAME",   (1, 5), (1, 5), "Helvetica-Bold"),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 0.5 * cm))

    return story


def _executive_summary(rpt: RCAReport, ss) -> List:
    story = [
        Paragraph("Executive Summary", ss["RCA_H1"]),
        HRFlowable(width="100%", thickness=1.5, color=C_DARK),
        Spacer(1, 0.2 * cm),
        Paragraph(rpt.summary, ss["RCA_Body"]),
        Spacer(1, 0.3 * cm),
    ]

    # Confidence
    story.append(Paragraph("Analysis Confidence", ss["RCA_H2"]))
    story.append(_confidence_bar(rpt.confidence))
    story.append(Spacer(1, 0.4 * cm))

    return story


def _root_cause_section(rpt: RCAReport, ss) -> List:
    story = [
        Paragraph("Root Cause Identification", ss["RCA_H1"]),
        HRFlowable(width="100%", thickness=1.5, color=C_DARK),
        Spacer(1, 0.2 * cm),
    ]

    if rpt.root_cause is None:
        story.append(Paragraph(
            "No upstream root cause was identified within the lookback window. "
            "The trigger anomaly appears to be an originating event.",
            ss["RCA_Body"]
        ))
        return story

    rc = rpt.root_cause
    rc_data = [
        ["Field", "Value"],
        ["Resource", rc.resource_name],
        ["Resource Type", rc.resource_type],
        ["Cloud / Region", f"{rc.cloud} / {rc.region}"],
        ["Metric", rc.metric_name],
        ["Observed Value", f"{rc.metric_value:.4f}"],
        ["Baseline Average", f"{rc.baseline_avg:.4f}"],
        ["Deviation", f"{rc.deviation_pct:.1f}%"],
        ["First Seen", rc.first_seen_at[:19].replace("T", " ")],
        ["Time Before Trigger", f"{rc.time_offset_seconds/60:.1f} minutes"],
        ["Correlation Score", f"{rc.correlation_score:.3f}"],
    ]

    t = _table(
        [[Paragraph(str(c), ss["RCA_Small"] if i == 0 else ss["RCA_Body"])
          for i, c in enumerate(row)]
         for row in rc_data],
        [4 * cm, 12.5 * cm],
        style_extra=[
            ("BACKGROUND", (0, 7), (-1, 7), colors.HexColor("#FFEBEE")),  # deviation row
        ]
    )
    story.append(t)
    story.append(Spacer(1, 0.5 * cm))

    return story


def _cascade_section(rpt: RCAReport, ss) -> List:
    if not rpt.cascading_effects:
        return []

    story = [
        Paragraph("Cascading Effects", ss["RCA_H1"]),
        HRFlowable(width="100%", thickness=1.5, color=C_DARK),
        Spacer(1, 0.2 * cm),
        Paragraph(
            "The following resources/metrics showed anomalous behavior during the incident window "
            "and may have been affected by the root cause.",
            ss["RCA_Body"]
        ),
        Spacer(1, 0.2 * cm),
    ]

    headers = ["Resource", "Type", "Metric", "Value", "Deviation", "Offset (min)", "Score"]
    rows = [headers]
    for c in rpt.cascading_effects:
        direction = "before" if c.time_offset_seconds > 0 else "after"
        rows.append([
            c.resource_name[:24],
            c.resource_type,
            c.metric_name[:22],
            f"{c.metric_value:.3f}",
            f"{c.deviation_pct:.0f}%",
            f"{abs(c.time_offset_seconds/60):.1f} {direction}",
            f"{c.correlation_score:.2f}",
        ])

    col_w = [4.5*cm, 2.5*cm, 3.5*cm, 1.8*cm, 1.8*cm, 2.2*cm, 1.5*cm]
    t = _table(
        [[Paragraph(str(cell), ss["RCA_Small"] if i == 0 else ss["RCA_Body"])
          for i, cell in enumerate(row)]
         for row in rows],
        col_w
    )
    story.append(t)
    story.append(Spacer(1, 0.5 * cm))
    return story


def _timeline_section(rpt: RCAReport, ss) -> List:
    if not rpt.timeline:
        return []

    story = [
        Paragraph("Incident Timeline", ss["RCA_H1"]),
        HRFlowable(width="100%", thickness=1.5, color=C_DARK),
        Spacer(1, 0.2 * cm),
    ]

    TYPE_COLORS = {
        "root_cause": C_RED,
        "trigger":    C_ORANGE,
        "cascade":    C_BLUE,
        "error_log":  colors.HexColor("#6A1B9A"),
    }

    rows = [["Time", "Event Type", "Description"]]
    style_extra = []

    for i, event in enumerate(rpt.timeline, start=1):
        t_str = event["time"][:19].replace("T", " ")
        evt_type = event["type"].replace("_", " ").title()
        rows.append([t_str, evt_type, event["label"][:80]])

        ec = TYPE_COLORS.get(event["type"], C_MUTED)
        style_extra.append(("BACKGROUND", (1, i), (1, i), ec))
        style_extra.append(("TEXTCOLOR",  (1, i), (1, i), C_WHITE))
        style_extra.append(("FONTNAME",   (1, i), (1, i), "Helvetica-Bold"))

    t = _table(
        [[Paragraph(str(cell), ss["RCA_Small"] if j == 0 else ss["RCA_Body"])
          for j, cell in enumerate(row)]
         for row in rows],
        [3.5*cm, 2.8*cm, 11.2*cm],
        style_extra=style_extra,
    )
    story.append(t)
    story.append(Spacer(1, 0.5 * cm))
    return story


def _error_logs_section(rpt: RCAReport, ss) -> List:
    if not rpt.related_errors:
        return []

    story = [
        Paragraph("Error Logs in Incident Window", ss["RCA_H1"]),
        HRFlowable(width="100%", thickness=1.5, color=C_DARK),
        Spacer(1, 0.2 * cm),
    ]

    rows = [["Time", "Resource", "Level", "Message"]]
    style_extra = []

    for i, e in enumerate(rpt.related_errors[:20], start=1):  # cap at 20
        rows.append([
            e.collected_at[:19].replace("T", " "),
            e.resource_name[:20],
            e.log_level,
            e.message[:70],
        ])
        if e.log_level in ("CRITICAL", "FATAL"):
            style_extra.append(("BACKGROUND", (2, i), (2, i), C_RED))
            style_extra.append(("TEXTCOLOR",  (2, i), (2, i), C_WHITE))
        elif e.log_level == "ERROR":
            style_extra.append(("BACKGROUND", (2, i), (2, i), C_ORANGE))
            style_extra.append(("TEXTCOLOR",  (2, i), (2, i), C_WHITE))

    t = _table(
        [[Paragraph(str(cell), ss["RCA_Code"] if j == 3 else ss["RCA_Small"])
          for j, cell in enumerate(row)]
         for row in rows],
        [3.5*cm, 3.5*cm, 1.8*cm, 8.7*cm],
        style_extra=style_extra,
    )
    story.append(t)
    story.append(Spacer(1, 0.5 * cm))
    return story


def _charts_section(rpt: RCAReport, ss) -> List:
    if not rpt.metric_history:
        return []

    story = [
        Paragraph("Metric Trend Charts", ss["RCA_H1"]),
        HRFlowable(width="100%", thickness=1.5, color=C_DARK),
        Spacer(1, 0.3 * cm),
    ]

    for key, history in rpt.metric_history.items():
        if not history:
            continue
        label = key.replace("::", " / ")
        chart = _metric_chart(history, label, width=16 * cm, height=4 * cm)
        if chart:
            story.append(chart)
            story.append(Spacer(1, 0.3 * cm))

    return story


def _actions_section(rpt: RCAReport, ss) -> List:
    story = [
        Paragraph("Recommended Actions", ss["RCA_H1"]),
        HRFlowable(width="100%", thickness=1.5, color=C_DARK),
        Spacer(1, 0.2 * cm),
    ]

    for i, action in enumerate(rpt.recommended_actions, start=1):
        story.append(Paragraph(f"{i}.  {action}", ss["RCA_Action"]))

    story.append(Spacer(1, 0.5 * cm))
    return story


# ── Main public function ───────────────────────────────────────────────────────

def generate_pdf(report: RCAReport, output_dir: str = "rca_reports") -> str:
    """
    Generate a PDF for the given RCAReport.
    Returns the path to the saved PDF file.
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{report.report_id}.pdf"
    out_path = str(Path(output_dir) / filename)

    ss = _styles()

    # Build story (content)
    story = []
    story += _cover_section(report, ss)
    story += _executive_summary(report, ss)
    story += _root_cause_section(report, ss)
    story += _cascade_section(report, ss)
    story += _timeline_section(report, ss)
    story += _error_logs_section(report, ss)
    story.append(PageBreak())
    story += _charts_section(report, ss)
    story += _actions_section(report, ss)

    # Final note
    story.append(HRFlowable(width="100%", thickness=0.5, color=C_BORDER))
    story.append(Spacer(1, 0.2 * cm))
    story.append(Paragraph(
        f"This report was auto-generated by the Multi-Cloud Observability RCA Engine. "
        f"Confidence: {report.confidence*100:.0f}%. "
        f"Report ID: {report.report_id}",
        ss["RCA_Small"]
    ))

    # Build PDF
    doc = BaseDocTemplate(
        out_path,
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=2.2 * cm, bottomMargin=1.6 * cm,
        title=f"RCA Report — {report.report_id}",
        author="Multi-Cloud Observability System",
    )

    # Attach metadata for header/footer
    doc.rca_report_id = report.report_id
    doc.generated_at  = report.generated_at[:19].replace("T", " ") + " UTC"

    frame = Frame(
        MARGIN, 1.6 * cm,
        PAGE_W - 2 * MARGIN, PAGE_H - 2.2 * cm - 1.6 * cm,
        id="main",
    )
    template = PageTemplate(id="main", frames=[frame], onPage=_header_footer)
    doc.addPageTemplates([template])

    doc.build(story)
    print(f"[PDF] Report saved: {out_path}")
    return out_path