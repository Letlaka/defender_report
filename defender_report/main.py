#!/usr/bin/env python3
"""
Command-line entry point for the DefenderAgents report generator.
"""

import os
import sys
import datetime
import argparse
import pandas as pd
import logging
from tqdm import tqdm

from defender_report.utils import configure_logging, Spinner
from defender_report.grouping import group_rows_by_department, load_sheet_order
from defender_report.categorization import categorize_dataframe, tally_dataframe
from defender_report.enrichment import enrich_all_sheets_with_ad
from defender_report.reporting import write_full_report, write_department_reports

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
        help="Path to DefenderAgents.xlsx; default=./DefenderAgents.xlsx"
    )
    parser.add_argument(
        "--template-path",
        default=os.path.join(os.getcwd(), "AVReport.xlsx"),
        help="Path to AVReport.xlsx; default=./AVReport.xlsx"
    )
    parser.add_argument(
        "--output-path",
        default=os.path.join(os.getcwd(), "DefenderAgents_Report.xlsx"),
        help="Where to write the master report; default=./DefenderAgents_Report.xlsx"
    )
    parser.add_argument(
        "--date",
        default=today_str,
        help="Reference date YYYY-MM-DD; default=today"
    )
    parser.add_argument(
        "--threshold-days",
        type=int,
        default=7,
        help="Days back considered UpToDate; default=7"
    )
    parser.add_argument(
        "--enrich-ad",
        action="store_true",
        help="Enable AD enrichment via PowerShell"
    )

    return parser.parse_args()

def main() -> None:
    configure_logging()
    args = parse_command_line_arguments()

    # Parse the reference date
    try:
        reference_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid --date format; expected YYYY-MM-DD")
        sys.exit(1)

    # Load the input sheet
    if not os.path.isfile(args.input_path):
        logger.error("Input not found: %s", args.input_path)
        sys.exit(1)
    with Spinner("Reading DefenderAgents.xlsx"):
        data_frame = pd.read_excel(args.input_path)

    if "UserName" not in data_frame.columns:
        logger.error("Input must contain 'UserName' column")
        sys.exit(1)

    # Group rows by department
    grouped_sheets = group_rows_by_department(data_frame)
    sheet_order = load_sheet_order(args.template_path)

    # Build per-department DataFrames and tally summary rows
    all_sheets = {}
    summary_rows = []
    departments = list(sheet_order)
    if "ungrouped" not in departments:
        departments.append("ungrouped")

    for department_name in tqdm(departments, desc="Processing departments", unit="dept"):
        chunk = grouped_sheets.get(department_name, pd.DataFrame(columns=data_frame.columns))
        categorized = categorize_dataframe(chunk, reference_date, args.threshold_days)
        all_sheets[department_name] = categorized

        required_columns = {"DeviceName", "_ManagedBy", "LastReportedDateTime"}
        if required_columns.issubset(categorized.columns):
            tally = tally_dataframe(categorized)
            tally["Department"] = department_name
            summary_rows.append(tally)
        else:
            logger.warning("Skipping tally for '%s': missing columns", department_name)

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

    # Map sheet-codes to display names
    display_map = {
        "gpedu": "EDUCATION", "gphealth": "HEALTH", "gpgded": "DED",
        "gdsd": "SocDev", "gpsports": "SPORTS", "gdard": "AGRIC",
        "gpt": "TREASURY", "gpdrt": "TRANSPORT", "gpegov": "EGOV",
        "gpdid": "DID", "gdhus": "GDHUS", "gpdpr": "OOP",
        "gpsas": "COMMSAFETY", "cogta": "COGTA",
    }
    summary_df["Department"] = (
        summary_df["Department"].map(display_map).fillna(summary_df["Department"])
    )

    # Optional AD enrichment
    if args.enrich_ad:
        logger.info("Enriching AD via PowerShell (this may take a few minutes)...")
        all_sheets = enrich_all_sheets_with_ad(all_sheets)
    else:
        logger.info("Skipping AD enrichment (no --enrich-ad flag)")

    # Write reports
    write_full_report(
        all_sheets, summary_df, sheet_order, args.output_path)
    output_directory = os.path.dirname(args.output_path) or os.getcwd()
    write_department_reports(all_sheets, summary_df, sheet_order, output_directory)

if __name__ == "__main__":
    main()
