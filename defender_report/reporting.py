import datetime
import logging
import os
from typing import Dict, List, Tuple, cast
from xlsxwriter.workbook import Workbook

import pandas as pd
from tqdm import tqdm

from defender_report.utils import make_datetime_columns_timezone_naive

logger = logging.getLogger(__name__)


def _nested_report_folder(
    root_directory: str, department: str, report_date: datetime.date
) -> str:
    """Build nested output path for a department report by date/quarter."""
    month = report_date.month
    year = report_date.year
    fy_start = year if month >= 4 else year - 1
    financial_year = f"{fy_start}-{fy_start + 1}"
    quarter = (
        "Q1"
        if 4 <= month <= 6
        else "Q2"
        if 7 <= month <= 9
        else "Q3"
        if 10 <= month <= 12
        else "Q4"
    )
    path_parts = [
        str(root_directory),
        str(department),
        str(financial_year),
        str(quarter),
        report_date.strftime("%B"),
        report_date.isoformat(),
    ]
    full_path = os.path.join(*path_parts)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def _write_table(
    writer: pd.ExcelWriter,
    dataframe: pd.DataFrame,
    sheet_name: str,
    style_name: str = "Table Style Medium 16",
) -> None:
    """Write a formatted table to an Excel sheet."""
    dataframe = make_datetime_columns_timezone_naive(dataframe)
    dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]
    rows, cols = dataframe.shape
    if rows > 0 and cols > 0:
        worksheet.add_table(
            0,
            0,
            rows,
            cols - 1,
            {
                "style": style_name,
                "columns": [{"header": c} for c in dataframe.columns],
            },
        )


def _write_summary_table(
    writer: pd.ExcelWriter,
    summary_dataframe: pd.DataFrame,
    sheet_name: str,
    report_date: datetime.date,
) -> None:
    """Write the summary sheet (always styled, always fixed columns)."""
    workbook:Workbook = writer.book  # type: ignore
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet

    # Formats
    date_fmt = workbook.add_format({
        "bold": True,
        "align": "center",
        "font_size": 14,
        "bg_color": "#4F81BD",
        "font_color": "#000000",
    })
    header_fmt = workbook.add_format({
        "bold": True,
        "align": "center",
        "valign": "vcenter",
        "font_color": "#FFFFFF",
        "bg_color": "#16365C",
        "border": 1,
    })
    cell_fmt = workbook.add_format({"border": 1, "align": "center"})
    green_fmt = workbook.add_format({
        "bg_color": "#00b050", "num_format": "0.0%", "border": 1, "align": "center"
    })
    yellow_fmt = workbook.add_format({
        "bg_color": "#ffff00", "num_format": "0.0%", "border": 1, "align": "center"
    })
    red_fmt = workbook.add_format({
        "bg_color": "#ff0000", "num_format": "0.0%", "border": 1, "align": "center"
    })

    col_count = len(summary_dataframe.columns)
    worksheet.merge_range(0, 0, 0, col_count - 1, report_date.strftime("%d-%b"), date_fmt)

    for col_idx, col in enumerate(summary_dataframe.columns):
        worksheet.write(1, col_idx, col, header_fmt)
        worksheet.set_column(col_idx, col_idx, 18)
    worksheet.autofilter(1, 0, 1, col_count - 1)

    # Table body with compliance coloring and zebra striping
    for row_idx, (_, row) in enumerate(summary_dataframe.iterrows()):
        excel_row = 2 + row_idx
        is_zebra = row_idx % 2 == 1
        for col_idx, col in enumerate(summary_dataframe.columns):
            value = row[col]
            fmt = cell_fmt
            if col.lower() == "compliance":
                try:
                    v = float(value)
                    if v >= 0.8:
                        fmt = green_fmt
                    elif v > 0.7:
                        fmt = yellow_fmt
                    else:
                        fmt = red_fmt
                except Exception:
                    fmt = cell_fmt
            elif is_zebra:
                fmt = workbook.add_format(
                    {"bg_color": "#F2F2F2", "border": 1, "align": "center"}
                )
            worksheet.write(excel_row, col_idx, value, fmt)

    # Legend block below table (leave a gap)
    legend_start = 2 + len(summary_dataframe) + 2
    legends = [
        ("Baseline 80%", cell_fmt),
        ("> 80% (green)", green_fmt),
        ("< 80% (yellow)", yellow_fmt),
        ("< 70% (red)", red_fmt),
    ]
    for i, (text, fmt) in enumerate(legends):
        worksheet.write(legend_start + i, 0, text, fmt)


def write_full_report(
    all_sheets: Dict[str, pd.DataFrame],
    summary_df: pd.DataFrame,
    sheet_order: List[str],
    output_path: str,
    include_ungrouped: bool = True,
) -> None:
    """
    Write the master Excel report.
    Only exports the *essential* columns in each department sheet.
    """
    # Department display names
    display_map = {
        "gdard": "AGRIC",
        "cogta": "COGTA",
        "gpsas": "COMMSAFETY",
        "gpgded": "DED",
        "gpdid": "DID",
        "gpedu": "EDUCATION",
        "gpegov": "EGOV",
        "gdhus": "GDHUS",
        "gphealth": "HEALTH",
        "gpdpr": "OOP",
        "gdsd": "SOCDEV",
        "gpsports": "SPORTS",
        "gpdrt": "TRANSPORT",
        "gpt": "TREASURY",
        "ungrouped": "ungrouped",
    }

    if include_ungrouped and "ungrouped" not in sheet_order:
        sheet_order = sheet_order + ["ungrouped"]

    # Only export the minimum columns for master (no advanced forensic/AD fields)
    essential_cols = [
        "DeviceName",
        "UserName",
        "LastReportedDateTime",
        "Status",
        "SignatureVersion",
        "SignatureLastUpdated",
        "EngineVersion",
        "PlatformVersion",
        "ComplianceLevel",
        "ComplianceSeverity",
        "ComplianceReason",
    ]

    report_date = datetime.date.today()
    logger.info("Starting master report: %s", output_path)

    with pd.ExcelWriter(output_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
        for dept_code in tqdm(sheet_order, desc="Master sheets", unit="sheet"):
            df = all_sheets.get(dept_code, pd.DataFrame())
            if not df.empty:
                cols_present: List[str] = [c for c in essential_cols if c in df.columns]
                extra_cols: List[str] = [c for c in df.columns if c not in cols_present]
                all_cols: List[str] = cols_present + extra_cols
                df_export = cast(pd.DataFrame, df.loc[:, all_cols])
                df_export = make_datetime_columns_timezone_naive(df_export)
                sheet_name = display_map.get(dept_code, dept_code)
                _write_table(writer, df_export, sheet_name)
            else:
                ws = writer.book.add_worksheet(display_map.get(dept_code, dept_code)) # type: ignore
                ws.write(0, 0, "No data for this department.")
                writer.sheets[display_map.get(dept_code, dept_code)] = ws

        _write_summary_table(writer, summary_df, "Summary", report_date)

    logger.info("Master report written to %s", output_path)


def write_department_reports(
    all_sheets: Dict[str, pd.DataFrame],
    summary_dataframe: pd.DataFrame,
    sheet_order: List[str],
    output_root: str,
    include_ungrouped: bool = True,
) -> List[Tuple[str, str]]:
    """
    Generate one workbook per department (full details).
    The 'Summary' sheet always uses columns A–H, with only the header and that department's row,
    and the compliance cell color-coded as per the legend.
    Returns a list of (department_code, path_to_file).
    """
    from defender_report.utils import make_datetime_columns_timezone_naive

    report_date = datetime.date.today()
    depts = list(sheet_order)
    if include_ungrouped and "ungrouped" not in depts:
        depts.append("ungrouped")

    summary_columns = [
        "Department",
        "DeviceCount",
        "Co-managed",
        "Intune Managed",
        "SCCM Managed",
        "Up to Date",
        "Out of Date",
        "Compliance",
    ]
    col_count = len(summary_columns)

    display_map = {
        "gdard": "AGRIC",
        "cogta": "COGTA",
        "gpsas": "COMMSAFETY",
        "gpgded": "DED",
        "gpdid": "DID",
        "gpedu": "EDUCATION",
        "gpegov": "EGOV",
        "gdhus": "GDHUS",
        "gphealth": "HEALTH",
        "gpdpr": "OOP",
        "gdsd": "SOCDEV",
        "gpsports": "SPORTS",
        "gpdrt": "TRANSPORT",
        "gpt": "TREASURY",
    }

    logger.info("Writing %d department reports to %s", len(depts), output_root)

    summaries: List[Tuple[str, str]] = []
    for dept in tqdm(depts, desc="Per-dept", unit="dept"):
        target = _nested_report_folder(output_root, dept, report_date)
        filename = f"{dept}_Report_{report_date.isoformat()}.xlsx"
        full_path = os.path.join(target, filename)

        with pd.ExcelWriter(full_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
            # Per-department: export all columns
            dept_data = all_sheets.get(dept, pd.DataFrame())
            if not dept_data.empty:
                dept_data = make_datetime_columns_timezone_naive(dept_data)
                _write_table(writer, dept_data, dept)
            else:
                worksheet = writer.book.add_worksheet(dept) # type: ignore
                worksheet.write(0, 0, "No data for this department.")
                writer.sheets[dept] = worksheet

            # Stylized summary for this department
            worksheet = writer.book.add_worksheet("Summary") # type: ignore
            writer.sheets["Summary"] = worksheet

            # --- Formats ---
            date_fmt = writer.book.add_format({ # type: ignore
                "bold": True,
                "align": "center",
                "font_size": 14,
                "bg_color": "#C9DAF8",
                "border": 1,
            })
            header_fmt = writer.book.add_format({ # type: ignore
                "bold": True,
                "align": "center",
                "valign": "vcenter",
                "font_color": "#FFFFFF",
                "bg_color": "#4F81BD",
                "border": 1,
            })# Safely resolve department summary row using both display name and fallback
            dept_summary_row = pd.DataFrame()
            if "Department" in summary_dataframe.columns:
                display_name = display_map.get(dept, dept)
                dept_summary_row = summary_dataframe[
                    summary_dataframe["Department"] == display_name
                ]
                if dept_summary_row.empty:
                    dept_summary_row = summary_dataframe[
                        summary_dataframe["Department"] == dept
                    ]
            else:
                logger.warning("Missing 'Department' column in summary_dataframe — skipping summary for '%s'", dept)

            normal_fmt = writer.book.add_format({ # type: ignore
                "align": "center",
                "valign": "vcenter",
                "border": 1,
            })
            green_fmt = writer.book.add_format({ # type: ignore
                "bg_color": "#00b050",
                "align": "center",
                "valign": "vcenter",
                "border": 1,
                "num_format": "0.0%",
                "bold": True,
            })
            yellow_fmt = writer.book.add_format({ # type: ignore
                "bg_color": "#ffff00",
                "align": "center",
                "valign": "vcenter",
                "border": 1,
                "num_format": "0.0%",
                "bold": True,
            })
            red_fmt = writer.book.add_format({ # type: ignore
                "bg_color": "#ff0000",
                "align": "center",
                "valign": "vcenter",
                "border": 1,
                "num_format": "0.0%",
                "bold": True,
            })

            worksheet.merge_range(0, 0, 0, col_count - 1, report_date.strftime("%d-%b"), date_fmt)
            for col_idx, col_name in enumerate(summary_columns):
                worksheet.write(1, col_idx, col_name, header_fmt)

            # Find the summary row for this department and write it (A3:H3)
            # Safely resolve department summary row using both display name and fallback
            dept_summary_row = pd.DataFrame()
            if "Department" in summary_dataframe.columns:
                display_name = display_map.get(dept, dept)
                dept_summary_row = summary_dataframe[
                    summary_dataframe["Department"] == display_name
                ]
                if dept_summary_row.empty:
                    dept_summary_row = summary_dataframe[
                        summary_dataframe["Department"] == dept
                    ]
            else:
                logger.warning("Missing 'Department' column in summary_dataframe — skipping summary for '%s'", dept)

            if not dept_summary_row.empty:
                row_vals = [
                    dept_summary_row[col].iloc[0] if col in dept_summary_row.columns else ""
                    for col in summary_columns
                ]
                for col_idx, (col_name, value) in enumerate(zip(summary_columns, row_vals)):
                    if col_name.lower() == "compliance":
                        try:
                            compliance_val = value
                        except Exception:
                            compliance_val = None
                        if compliance_val is not None and compliance_val >= 0.8: # type: ignore
                            fmt = green_fmt
                        elif compliance_val is not None and 0.7 < compliance_val < 0.8: # type: ignore
                            fmt = yellow_fmt
                        elif compliance_val is not None and compliance_val <= 0.7: # type: ignore
                            fmt = red_fmt
                        else:
                            fmt = normal_fmt
                        worksheet.write(2, col_idx, value, fmt)
                    else:
                        worksheet.write(2, col_idx, value, normal_fmt)
            else:
                worksheet.write(
                    2, 0, "No summary data available for this department.", header_fmt
                )

            for col_idx in range(col_count):
                worksheet.set_column(col_idx, col_idx, 16)
            worksheet.freeze_panes(3, 0)

            # Legend
            legend = [
                ("Baseline 80%", None),
                ("> 80% (green)", "#00b050"),
                ("< 80% (yellow)", "#ffff00"),
                ("< 70% (red)", "#ff0000"),
            ]
            row0 = 4
            for i, (text, color) in enumerate(legend):
                props = {"bold": True, "align": "left"}
                if color:
                    props["bg_color"] = color
                fmt = writer.book.add_format(props) # type: ignore
                worksheet.write(row0 + i, 0, text, fmt)

        summaries.append((dept, full_path))

    return summaries
