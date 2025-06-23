#!/usr/bin/env python3
"""
Command-line entry point for the DefenderAgents report generator.
"""

import argparse
import datetime
import logging
import os
import sys
import shutil
from typing import List


import pandas as pd
from tqdm import tqdm

from defender_report.categorization import categorize_dataframe, tally_dataframe
from defender_report.enrichment import enrich_all_sheets_with_ad
from defender_report.grouping import group_rows_by_department, load_sheet_order
from defender_report.reporting import write_department_reports, write_full_report
from defender_report.utils import Spinner, configure_logging

logger = logging.getLogger(__name__)


def parse_command_line_arguments() -> argparse.Namespace:
    """
    Define and parse all CLI arguments.
    """
    today_str = datetime.date.today().isoformat()
    parser = argparse.ArgumentParser(__doc__)

    parser.add_argument(
        "--input-path",
        default=os.path.join(os.getcwd(), "DefenderAgents.xlsx"),
        help="Path to DefenderAgents.xlsx; default=./DefenderAgents.xlsx",
    )
    parser.add_argument(
        "--template-path",
        default=os.path.join(os.getcwd(), "AVReport.xlsx"),
        help="Path to AVReport.xlsx; default=./AVReport.xlsx",
    )
    parser.add_argument(
        "--output-path",
        default=os.path.join(os.getcwd(), "DefenderAgents_Report.xlsx"),
        help="Where to write the master report; default=./DefenderAgents_Report.xlsx",
    )
    parser.add_argument(
        "--date",
        default=today_str,
        help="Reference date YYYY-MM-DD; default=today",
    )
    parser.add_argument(
        "--threshold-days",
        type=int,
        default=7,
        help="Days back considered UpToDate; default=7",
    )
    parser.add_argument(
        "--enrich-ad",
        action="store_true",
        help="Enable AD enrichment via PowerShell",
    )
    parser.add_argument(
        "--department",
        nargs="+",  # ← allow one or more codes
        metavar="DEPT",
        help=(
            "Optional: only generate reports for these departments "
            "(space-separated sheet codes, e.g. 'gpegov gpsas')"
        ),
    )
    parser.add_argument(
        "--master-only",
        action="store_true",
        help="Only write the master report; skip per-department workbooks",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional path to write a rotating log file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_command_line_arguments()
    configure_logging(log_file_path=args.log_file)

    # --- Parse reference date ---
    try:
        reference_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid --date format; expected YYYY-MM-DD")
        sys.exit(1)

    # --- Load the input sheet ---
    if not os.path.isfile(args.input_path):
        logger.error("Input not found: %s", args.input_path)
        sys.exit(1)

    with Spinner("Reading DefenderAgents.xlsx"):
        data_frame = pd.read_excel(args.input_path)
    logger.info("Loaded %d rows from %s", len(data_frame), args.input_path)

    if "UserName" not in data_frame.columns:
        logger.error("Input must contain 'UserName' column")
        sys.exit(1)

    # --- Group rows & load template order ---
    grouped_sheets = group_rows_by_department(data_frame)
    sheet_order = load_sheet_order(args.template_path)

    # --- Handle --department filtering ---
    if args.department:
        # Validate each requested code
        invalid = [
            dept
            for dept in args.department
            if dept not in sheet_order and dept != "ungrouped"
        ]
        if invalid:
            logger.error(
                "Unrecognized department(s): %s; valid codes are: %s",
                ", ".join(invalid),
                ", ".join(sheet_order + ["ungrouped"]),
            )
            sys.exit(1)

        logger.info("Limiting report to department(s): %s", args.department)
        # Only generate for those codes
        sheet_order = list(args.department)

    # --- Build per-department DataFrames & tally rows ---
    all_sheets: dict[str, pd.DataFrame] = {}
    summary_rows: list[dict] = []

    if args.department:
        departments = list(args.department)
    else:
        departments = list(sheet_order)
        if "ungrouped" not in departments:
            departments.append("ungrouped")

    for dept_code in tqdm(departments, desc="Processing departments", unit="dept"):
        chunk = grouped_sheets.get(dept_code, pd.DataFrame(columns=data_frame.columns))
        categorized = categorize_dataframe(chunk, reference_date, args.threshold_days)
        all_sheets[dept_code] = categorized

        required_columns = {"DeviceName", "_ManagedBy", "LastReportedDateTime"}
        if required_columns.issubset(categorized.columns):
            tally = tally_dataframe(categorized)
            tally["Department"] = dept_code
            summary_rows.append(tally)
        else:
            logger.warning("Skipping tally for '%s': missing columns", dept_code)

    summary_df = pd.DataFrame(summary_rows)[
        [
            "Department",
            "DeviceCount",
            "Co-managed",
            "Intune",
            "SCCM Managed",
            "inactive_av_clients",
            "Up to Date",
            "Out of Date",
            "Compliance",
        ]
    ]

    # --- Map codes to display names & filter summary if needed ---
    display_map = {
        "gpedu": "EDUCATION",
        "gphealth": "HEALTH",
        "gpgded": "DED",
        "gdsd": "SocDev",
        "gpsports": "SPORTS",
        "gdard": "AGRIC",
        "gpt": "TREASURY",
        "gpdrt": "TRANSPORT",
        "gpegov": "EGOV",
        "gpdid": "DID",
        "gdhus": "GDHUS",
        "gpdpr": "OOP",
        "gpsas": "COMMSAFETY",
        "cogta": "COGTA",
    }
    summary_df["Department"] = (
        summary_df["Department"].map(display_map).fillna(summary_df["Department"])
    )

    if args.department:
        # Only keep rows for the requested display names
        wanted = [display_map.get(code, code) for code in args.department]
        summary_df = summary_df[summary_df["Department"].isin(wanted)]

    # --- Optional AD enrichment ---
    if args.enrich_ad:
        logger.info("Enriching AD via PowerShell (this may take a few minutes)…")
        all_sheets = enrich_all_sheets_with_ad(all_sheets)
    else:
        logger.info("Skipping AD enrichment (no --enrich-ad flag)")

    # --- Write master & per-department reports ---
    write_full_report(
        all_sheets,
        summary_df,
        sheet_order,
        args.output_path,
        include_ungrouped=(args.department is None),
    )

    dept_summaries: List[tuple[str,str]] = []
    if not args.master_only:
        output_directory = os.path.dirname(args.output_path) or os.getcwd()
        dept_summaries = write_department_reports(
            all_sheets,
            summary_df,
            sheet_order,
            output_directory,
            include_ungrouped=(args.department is None),
        ) or []
    else:
        logger.info("Skipping individual department reports (--master-only)")
    # ── print a final summary table ──────────────────────────
    try:
        from tabulate import tabulate
    except ImportError:
        logger.warning("Install tabulate (`pip install tabulate`) to see a summary table")
    else:
        print()  # blank line for spacing
        print("✓ Reports complete:\n")
        table = [("Master", args.output_path)] + dept_summaries
        print(tabulate(table, headers=["Report", "Path"], tablefmt="github"))
        print()

    # wait for user input before exiting
    if os.name == "nt":
        # get console width (fallback to 80 cols)
        width = shutil.get_terminal_size((80, 20)).columns

        msg = "✅  Reports complete  ✅"
        border = "=" * width

        # print a blank line, then top border, centered message, bottom border, blank line
        print()
        print(border)
        print(msg.center(width))
        print(border)
        print()
        input("Press ENTER to close this window…")

if __name__ == "__main__":
    main()
