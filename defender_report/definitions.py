# defender_report/definitions.py
import datetime
import pandas as pd
from typing import Tuple
from openpyxl.chart import PieChart, Reference
from openpyxl.chart.series import DataPoint
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.workbook import Workbook

# Labels in display order
CATEGORY_LABELS = [
    "Current",
    "From 1 through 3 days",
    "From 3 through 7 days",
    "Older than 7 days",
    "No definition found",
]

# Colors in same order as your screenshot
CATEGORY_COLORS = [
    "4472C4",  # Current (blue)
    "ED7D31",  # 1–3 days (orange)
    "A52A2A",  # 3–7 days (brown/red)
    "305496",  # Older than 7 days (dark blue)
    "7F7F7F",  # No definition found (grey)
]


def _resolve_age_days(row, report_date: datetime.date) -> Tuple[bool, int]:
    """
    Return (has_def, age_days) using ONLY LastReportedDateTime.
    """
    if "LastReportedDateTime" in row and pd.notnull(row["LastReportedDateTime"]):
        try:
            dt = pd.to_datetime(row["LastReportedDateTime"]).date()
            return True, (report_date - dt).days
        except Exception:
            pass
    return False, -1


def _bucket_label(has_def: bool, age_days: int) -> str:
    """
    Map (has_def, age_days) → category label.
    """
    if not has_def:
        return "No definition found"
    if age_days <= 0:
        return "Current"
    if 1 <= age_days <= 3:
        return "From 1 through 3 days"
    if 3 <= age_days <= 7:
        return "From 3 through 7 days"
    return "Older than 7 days"


def build_definition_summary(
    df: pd.DataFrame, report_date: datetime.date
) -> pd.DataFrame:
    """
    Build Status/Count summary DataFrame based only on LastReportedDateTime.
    """
    if df is None or df.empty:
        return pd.DataFrame({"Status": CATEGORY_LABELS, "Count": [0, 0, 0, 0, 0]})

    labels = []
    for _, row in df.iterrows():
        has_def, age_days = _resolve_age_days(row, report_date)
        labels.append(_bucket_label(has_def, age_days))

    counts = pd.Series(labels).value_counts()
    return pd.DataFrame(
        [(label, int(counts.get(label, 0))) for label in CATEGORY_LABELS],
        columns=["Status", "Count"],
    )

def write_definition_summary_sheet(
    writer,
    summary_df: pd.DataFrame,
    sheet_name: str = "Definition Summary",
    chart_title: str = "Definition status on computers",
) -> None:
    """
    Writes the Status/Count table and a pie chart to a new sheet using XlsxWriter.
    Compatible with pd.ExcelWriter(engine="xlsxwriter").
    """
    workbook  = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet

    # Formats
    header_format = workbook.add_format({"bold": True, "bg_color": "#D9D9D9", "border": 1})
    cell_format   = workbook.add_format({"border": 1})
    num_format    = workbook.add_format({"border": 1, "num_format": "0"})

    # Write headers
    worksheet.write(0, 0, "Status", header_format)
    worksheet.write(0, 1, "Count", header_format)

    # Write rows
    for idx, row in enumerate(summary_df.itertuples(index=False), start=1):
        worksheet.write(idx, 0, row.Status, cell_format)
        worksheet.write_number(idx, 1, int(row.Count), num_format)

    # Create pie chart
    chart = workbook.add_chart({"type": "pie"})
    chart.add_series({
        "name":       chart_title,
        "categories": [sheet_name, 1, 0, len(summary_df), 0],  # Status labels
        "values":     [sheet_name, 1, 1, len(summary_df), 1],  # Count values
        "points": [
            {"fill": {"color": "#70AD47"}},  # Current
            {"fill": {"color": "#A9D18E"}},  # 1-3 days
            {"fill": {"color": "#FFD966"}},  # 3-7 days
            {"fill": {"color": "#ED7D31"}},  # >7 days
            {"fill": {"color": "#FF0000"}},  # No definition
        ],
        "data_labels": {"percentage": True, "category": True},
    })

    chart.set_title({"name": chart_title})
    chart.set_style(10)

    # Insert chart to the right of the table
    worksheet.insert_chart(1, 3, chart, {"x_scale": 1.5, "y_scale": 1.5})

    # Adjust column widths
    worksheet.set_column(0, 0, 28)
    worksheet.set_column(1, 1, 10)
