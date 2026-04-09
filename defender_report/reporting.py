import datetime
import logging
import os
from typing import Dict, List, Tuple, cast

import pandas as pd
from tqdm import tqdm
from xlsxwriter.workbook import Workbook

from defender_report.definitions import (
    build_definition_summary,
    write_definition_summary_sheet,
)
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
    workbook: Workbook = writer.book  # type: ignore
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet

    # Formats
    date_fmt = workbook.add_format(
        {
            "bold": True,
            "align": "center",
            "font_size": 14,
            "bg_color": "#4F81BD",
            "font_color": "#000000",
        }
    )
    header_fmt = workbook.add_format(
        {
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "font_color": "#FFFFFF",
            "bg_color": "#16365C",
            "border": 1,
        }
    )
    cell_fmt = workbook.add_format({"border": 1, "align": "center"})
    green_fmt = workbook.add_format(
        {"bg_color": "#00b050", "num_format": "0.0%", "border": 1, "align": "center"}
    )
    yellow_fmt = workbook.add_format(
        {"bg_color": "#ffff00", "num_format": "0.0%", "border": 1, "align": "center"}
    )
    red_fmt = workbook.add_format(
        {"bg_color": "#ff0000", "num_format": "0.0%", "border": 1, "align": "center"}
    )

    col_count = len(summary_dataframe.columns)
    worksheet.merge_range(
        0, 0, 0, col_count - 1, report_date.strftime("%d-%b"), date_fmt
    )

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
    Adds a 'Definition Summary' sheet with pie chart for all devices.
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
        "environment": "Environment",
        "ungrouped": "ungrouped",
    }

    # Ensure any dynamically discovered sheets (e.g. newly added depts)
    # are included after the template order so they are exported.
    extra_sheets = [s for s in all_sheets.keys() if s not in sheet_order]
    if (
        include_ungrouped
        and "ungrouped" not in extra_sheets
        and "ungrouped" not in sheet_order
    ):
        extra_sheets.append("ungrouped")
    final_sheet_order = list(sheet_order) + sorted(extra_sheets)

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

    with pd.ExcelWriter(
        output_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
    ) as writer:
        # Write each department sheet
        for dept_code in tqdm(final_sheet_order, desc="Master sheets", unit="sheet"):
            df = all_sheets.get(dept_code, pd.DataFrame())
            if not df.empty:
                cols_present = [c for c in essential_cols if c in df.columns]
                extra_cols = [c for c in df.columns if c not in cols_present]
                all_cols = cols_present + extra_cols
                df_export = cast(pd.DataFrame, df.loc[:, all_cols])
                df_export = make_datetime_columns_timezone_naive(df_export)
                sheet_name = display_map.get(dept_code, dept_code)
                _write_table(writer, df_export, sheet_name)
            else:
                ws = writer.book.add_worksheet(display_map.get(dept_code, dept_code))  # type: ignore
                ws.write(0, 0, "No data for this department.")
                writer.sheets[display_map.get(dept_code, dept_code)] = ws

        # Summary sheet
        _write_summary_table(writer, summary_df, "Summary", report_date)

        # Definition Summary for ALL devices
        frames = [df for df in all_sheets.values() if not df.empty]
        all_devices_df = (
            pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        )
        def_summary_df = build_definition_summary(all_devices_df, report_date)
        write_definition_summary_sheet(
            writer,
            def_summary_df,
            sheet_name="Definition Summary",
            chart_title="Definition status on computers (All Devices)",
        )

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
    Adds a 'Definition Summary' sheet with pie chart for that department only.
    """
    report_date = datetime.date.today()
    # Start with template order, then add any additional discovered sheets
    depts = list(sheet_order)
    extra = [s for s in all_sheets.keys() if s not in depts]
    if include_ungrouped and "ungrouped" not in extra and "ungrouped" not in depts:
        extra.append("ungrouped")
    depts.extend(sorted(extra))

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
        "environment": "Environment",
        "ungrouped": "ungrouped",
    }

    logger.info("Writing %d department reports to %s", len(depts), output_root)

    summaries: List[Tuple[str, str]] = []
    for dept in tqdm(depts, desc="Per-dept", unit="dept"):
        target = _nested_report_folder(output_root, dept, report_date)
        filename = f"{dept}_Report_{report_date.isoformat()}.xlsx"
        full_path = os.path.join(target, filename)

        with pd.ExcelWriter(
            full_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
        ) as writer:
            # Department data
            dept_data = all_sheets.get(dept, pd.DataFrame())
            if not dept_data.empty:
                dept_data = make_datetime_columns_timezone_naive(dept_data)
                _write_table(writer, dept_data, dept)
            else:
                worksheet = writer.book.add_worksheet(dept)  # type: ignore
                worksheet.write(0, 0, "No data for this department.")
                writer.sheets[dept] = worksheet

            # Summary sheet styling
            worksheet = writer.book.add_worksheet("Summary")  # type: ignore
            writer.sheets["Summary"] = worksheet

            date_fmt = writer.book.add_format(
                {
                    "bold": True,
                    "align": "center",
                    "font_size": 14,
                    "bg_color": "#C9DAF8",
                    "border": 1,
                }
            )  # type: ignore
            header_fmt = writer.book.add_format(
                {
                    "bold": True,
                    "align": "center",
                    "valign": "vcenter",
                    "font_color": "#FFFFFF",
                    "bg_color": "#4F81BD",
                    "border": 1,
                }
            )  # type: ignore
            normal_fmt = writer.book.add_format(
                {"align": "center", "valign": "vcenter", "border": 1}
            )  # type: ignore
            green_fmt = writer.book.add_format(
                {
                    "bg_color": "#00b050",
                    "align": "center",
                    "valign": "vcenter",
                    "border": 1,
                    "num_format": "0.0%",
                    "bold": True,
                }
            )  # type: ignore
            yellow_fmt = writer.book.add_format(
                {
                    "bg_color": "#ffff00",
                    "align": "center",
                    "valign": "vcenter",
                    "border": 1,
                    "num_format": "0.0%",
                    "bold": True,
                }
            )  # type: ignore
            red_fmt = writer.book.add_format(
                {
                    "bg_color": "#ff0000",
                    "align": "center",
                    "valign": "vcenter",
                    "border": 1,
                    "num_format": "0.0%",
                    "bold": True,
                }
            )  # type: ignore

            worksheet.merge_range(
                0, 0, 0, col_count - 1, report_date.strftime("%d-%b"), date_fmt
            )
            for col_idx, col_name in enumerate(summary_columns):
                worksheet.write(1, col_idx, col_name, header_fmt)

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

            if not dept_summary_row.empty:
                row_vals = [
                    dept_summary_row[col].iloc[0]
                    if col in dept_summary_row.columns
                    else ""
                    for col in summary_columns
                ]
                for col_idx, (col_name, value) in enumerate(
                    zip(summary_columns, row_vals)
                ):
                    if col_name.lower() == "compliance":
                        if value is not None and value >= 0.8:
                            fmt = green_fmt
                        elif value is not None and 0.7 < value < 0.8:
                            fmt = yellow_fmt
                        elif value is not None and value <= 0.7:
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
                fmt = writer.book.add_format(props)  # type: ignore
                worksheet.write(row0 + i, 0, text, fmt)

            # Definition Summary for this department only
            def_summary_df = build_definition_summary(dept_data, report_date)
            write_definition_summary_sheet(
                writer,
                def_summary_df,
                sheet_name="Definition Summary",
                chart_title=f"Definition status on computers ({dept})",
            )

        summaries.append((dept, full_path))

    return summaries
