# defender_report/reporting.py
import datetime
import logging
import os
from typing import Dict, List, Tuple

import pandas as pd
from openpyxl.utils import get_column_letter
from tqdm import tqdm

logger = logging.getLogger(__name__)


def _nested_report_folder(
    root_directory: str, department: str, report_date: datetime.date
) -> str:
    """
    Build and return a nested folder path:
      root_directory/
        department/
          {financial_year}/
            Q{1–4}/
              {MonthName}/
                {YYYY-MM-DD}/
    """
    month = report_date.month
    year = report_date.year

    # financial year
    fy_start = year if month >= 4 else year - 1
    financial_year = f"{fy_start}-{fy_start + 1}"

    # quarter
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
        root_directory,
        department,
        financial_year,
        quarter,
        report_date.strftime("%B"),
        report_date.isoformat(),
    ]
    full_path = os.path.join(*path_parts)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def _create_compliance_formats(workbook):
    green = workbook.add_format({"bg_color": "#00b050", "num_format": "0.0%"})
    yellow = workbook.add_format({"bg_color": "#ffff00", "num_format": "0.0%"})
    red = workbook.add_format({"bg_color": "#ff0000", "num_format": "0.0%"})
    return green, yellow, red


def _write_table(
    writer: pd.ExcelWriter,
    dataframe: pd.DataFrame,
    sheet_name: str,
    style_name: str = "Table Style Medium 16",
) -> None:
    dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]
    rows, cols = dataframe.shape
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
    workbook = writer.book
    green, yellow, red = _create_compliance_formats(workbook)

    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet
    last_col_idx = len(summary_dataframe.columns) - 1

    # Date in A1
    date_fmt = workbook.add_format({"bold": True, "align": "left", "font_size": 12})
    raw = report_date.strftime("%d-%b").lstrip("0")
    worksheet.write(0, 0, raw, date_fmt)

    # DataFrame as a table with totals and conditional formatting
    start_row = 1
    summary_dataframe.to_excel(
        writer, sheet_name=sheet_name, index=False, startrow=start_row
    )
    n_rows, n_cols = summary_dataframe.shape

    # total row config
    cols_cfg = []
    for col in summary_dataframe.columns:
        if col == "Department":
            cols_cfg.append({"header": col})
        elif col == "Compliance":
            cols_cfg.append({"header": col, "total_function": "average"})
        else:
            cols_cfg.append({"header": col, "total_function": "sum"})

    worksheet.add_table(
        start_row,
        0,
        start_row + n_rows,
        n_cols - 1,
        {
            "style": "Table Style Medium 16",
            "total_row": True,
            "columns": cols_cfg,
        },
    )

    # conditional formatting on “Compliance” column
    comp_idx = summary_dataframe.columns.get_loc("Compliance") + 1
    col_letter = get_column_letter(comp_idx)
    data_start = start_row + 1
    data_end = start_row + n_rows
    rng = f"{col_letter}{data_start + 1}:{col_letter}{data_end + 1}"
    worksheet.conditional_format(
        rng, {"type": "cell", "criteria": ">=", "value": 0.8, "format": green}
    )
    worksheet.conditional_format(
        rng,
        {
            "type": "cell",
            "criteria": "between",
            "minimum": 0.7000001,
            "maximum": 0.8,
            "format": yellow,
        },
    )
    worksheet.conditional_format(
        rng, {"type": "cell", "criteria": "<", "value": 0.7, "format": red}
    )

    # legend
    legend = [
        ("Baseline 80%", None),
        ("> 80% (green)", "#00b050"),
        ("< 80% (yellow)", "#ffff00"),
        ("< 70% (red)", "#ff0000"),
    ]
    row0 = data_end + 2
    for i, (text, color) in enumerate(legend):
        props = {"bold": True, "align": "left"}
        if color:
            props["bg_color"] = color
        fmt = workbook.add_format(props)
        worksheet.write(row0 + i, 0, text, fmt)


def write_full_report(
    all_sheets: Dict[str, pd.DataFrame],
    summary_df: pd.DataFrame,
    sheet_order: List[str],
    output_path: str,
    include_ungrouped: bool = True,
) -> None:
    # ensure “ungrouped” appears last if requested
    if include_ungrouped and "ungrouped" not in sheet_order:
        sheet_order = sheet_order + ["ungrouped"]

    report_date = datetime.date.today()
    logger.info("Starting master report: %s", output_path)

    with pd.ExcelWriter(
        output_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
    ) as writer:
        wb = writer.book

        # per-department sheets
        for dept in tqdm(sheet_order, desc="Master sheets", unit="sheet"):
            df = all_sheets.get(dept, pd.DataFrame())
            df.to_excel(writer, sheet_name=dept, index=False)
            ws = writer.sheets[dept]
            r, c = df.shape
            ws.add_table(
                0,
                0,
                r,
                c - 1,
                {
                    "style": "Table Style Medium 16",
                    "columns": [{"header": h} for h in df.columns],
                },
            )

        # summary sheet
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
    Generate one report per department in a nested folder tree,
    using the same summary‐sheet helper for identical styling.
    Returns a list of (department_code, path_to_file).
    """
    report_date = datetime.date.today()
    depts = list(sheet_order)
    if include_ungrouped and "ungrouped" not in depts:
        depts.append("ungrouped")

    logger.info("Writing %d department reports to %s", len(depts), output_root)

    summaries: List[Tuple[str, str]] = []
    for dept in tqdm(depts, desc="Per-dept", unit="dept"):
        target = _nested_report_folder(output_root, dept, report_date)
        filename = f"{dept}_Report_{report_date.isoformat()}.xlsx"
        full_path = os.path.join(target, filename)

        with pd.ExcelWriter(
            full_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd"
        ) as writer:
            _write_table(writer, all_sheets.get(dept, pd.DataFrame()), dept)
            _write_summary_table(writer, summary_dataframe, "Summary", report_date)

        # ← append inside the loop
        summaries.append((dept, full_path))

    # ← return after the loop, properly indented
    return summaries