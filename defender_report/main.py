#!/usr/bin/env python3
"""
Command-line entry point for the DefenderAgents report generator.
"""

import argparse
import datetime
import logging
import os
import pathlib
import shutil
import sys
from typing import Dict, List
import json

from defender_report.emailer import send_email
import pandas as pd
from dotenv import load_dotenv

from defender_report.categorization import categorize_dataframe, tally_dataframe
from defender_report.grouping import group_rows_by_department, load_sheet_order
from defender_report.reporting import write_department_reports, write_full_report
from defender_report.utils import Spinner, configure_logging

logger = logging.getLogger(__name__)


# Environment setup
def resource_path(relative_path: str) -> str:
    """Get absolute path to resource (handles PyInstaller _MEIPASS)."""
    base = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base, relative_path)


env_path = resource_path(".env")
load_dotenv(env_path)

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
default_map = os.getenv("EMAILS_CONFIG", resource_path("emails_config.json"))

DISPLAY_MAP = {
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


def parse_command_line_arguments() -> argparse.Namespace:
    """Define and parse all CLI arguments."""
    today_str = datetime.date.today().isoformat()
    parser = argparse.ArgumentParser(__doc__)

    # Core I/O
    parser.add_argument(
        "--input-path", default=os.path.join(os.getcwd(), "DefenderAgents.xlsx")
    )
    parser.add_argument("--template-path", default=resource_path("AVReport.xlsx"))

    parser.add_argument(
        "--output-path", default=os.path.join(os.getcwd(), "DefenderAgents_Report.xlsx")
    )

    # AD enrichment
    parser.add_argument(
        "--enrich-ad", action="store_true", help="Enable AD enrichment (if available)"
    )
    parser.add_argument(
        "--use-cache", action="store_true", help="Use cached AD results if available"
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear cached AD results before querying",
    )
    parser.add_argument(
        "--export-unmatched-dir",
        type=str,
        help="Directory to save unmatched_devices.(csv|json)",
    )

    # Processing options
    parser.add_argument(
        "--date",
        default=today_str,
        help="Reference date for compliance checks (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--threshold-days",
        type=int,
        default=7,
        help="Days since last seen before 'Out of Date'",
    )
    parser.add_argument(
        "--department",
        nargs="+",
        metavar="DEPT",
        help="Filter to specific department(s)",
    )
    parser.add_argument(
        "--master-only",
        action="store_true",
        help="Only generate the master summary report",
    )

    # Emailing
    parser.add_argument(
        "--no-emails",
        dest="send_emails",
        action="store_false",
        help="Disable email sending",
    )
    parser.set_defaults(send_emails=True)
    parser.add_argument(
        "--emails-config",
        default=default_map,
        help="JSON file mapping department to emails",
    )
    parser.add_argument("--smtp-server", default=os.getenv("SMTP_SERVER"))
    parser.add_argument(
        "--smtp-port", type=int, default=int(os.getenv("SMTP_PORT", 587))
    )
    parser.add_argument("--smtp-user", default=os.getenv("SMTP_USER"))
    parser.add_argument("--smtp-password", default=os.getenv("SMTP_PASSWORD"))
    parser.add_argument("--from-email", default=os.getenv("FROM_EMAIL"))

    # Debug & runtime behavior
    parser.add_argument("--log-file", default=None, help="Optional log file path")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing reports or sending emails",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose debug logging"
    )
    parser.add_argument(
        "--resource-root",
        type=str,
        help="Override static resource directory (.env, template)",
    )
    parser.add_argument(
        "--open-output",
        action="store_true",
        help="Open report folder when done (Windows only)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_command_line_arguments()
    configure_logging(log_file_path=args.log_file)

    # Parse date
    try:
        reference_date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid --date format; expected YYYY-MM-DD")
        sys.exit(1)

    # Load input data
    if not os.path.isfile(args.input_path):
        logger.error("Input not found: %s", args.input_path)
        sys.exit(1)
    with Spinner("Reading DefenderAgents.xlsx"):
        data_frame = pd.read_excel(args.input_path)
    logger.info("Loaded %d rows from %s", len(data_frame), args.input_path)
    if "UserName" not in data_frame.columns:
        logger.error("Input must contain 'UserName' column")
        sys.exit(1)

    # Group by department and load template order
    grouped_sheets = group_rows_by_department(data_frame)
    sheet_order = load_sheet_order(args.template_path)

    # Handle department filtering
    if args.department:
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
        sheet_order = list(args.department)

    # Per-department logic
    all_sheets: Dict[str, pd.DataFrame] = {}
    summary_rows: List[dict] = []

    if args.department:
        departments = list(args.department)
    else:
        departments = list(sheet_order)
        if "ungrouped" not in departments:
            departments.append("ungrouped")

    # Enrich with AD if requested
    output_directory = os.path.dirname(args.output_path) or os.getcwd()

    if args.enrich_ad:
        try:
            from defender_report.enrichment import enrich_all_sheets_with_ad

            logger.info("Running AD enrichment...")
            all_sheets = enrich_all_sheets_with_ad(
                {
                    dept_code: grouped_sheets.get(
                        dept_code, pd.DataFrame(columns=data_frame.columns)
                    )
                    for dept_code in departments
                },
                export_dir=output_directory,
            )
        except Exception as e:
            logger.warning("AD enrichment failed or not available: %s", e)
            all_sheets = {
                dept_code: grouped_sheets.get(
                    dept_code, pd.DataFrame(columns=data_frame.columns)
                )
                for dept_code in departments
            }
    else:
        all_sheets = {
            dept_code: grouped_sheets.get(
                dept_code, pd.DataFrame(columns=data_frame.columns)
            )
            for dept_code in departments
        }

    # Categorize and tally
    for dept_code, chunk in all_sheets.items():
        categorized = categorize_dataframe(chunk, reference_date, args.threshold_days)
        all_sheets[dept_code] = categorized
        required_columns = {"DeviceName", "_ManagedBy", "LastReportedDateTime"}
        if required_columns.issubset(categorized.columns):
            tally = tally_dataframe(categorized)
            tally["Department"] = dept_code # type: ignore
            summary_rows.append(tally)
        else:
            logger.warning("Skipping tally for '%s': missing columns", dept_code)

    # Build summary DataFrame
    expected_summary_columns = [
        "Department",
        "DeviceCount",
        "Co-managed",
        "Intune",
        "SCCM Managed",
        "Up to Date",
        "Out of Date",
        "Compliance",
    ]
    summary_df = pd.DataFrame(summary_rows)

    missing_columns = [
        col for col in expected_summary_columns if col not in summary_df.columns
    ]
    if missing_columns:
        logger.warning("Missing expected summary columns: %s", ", ".join(missing_columns))
        for col in missing_columns:
            summary_df[col] = None  # Fill missing ones with None

    summary_df = summary_df[expected_summary_columns]
    summary_df.rename(columns={"Intune": "Intune Managed"}, inplace=True) # type: ignore

    if "Department" in summary_df.columns:
        summary_df["Department"] = (
            summary_df["Department"].map(DISPLAY_MAP).fillna(summary_df["Department"])
        )
    else:
        logger.warning(
            "Cannot map departments — 'Department' column not found. Available columns: %s",
            list(summary_df.columns),
        )

    # Prepare codes for all reports
    master_dept_codes = list(load_sheet_order(args.template_path))
    if "ungrouped" not in master_dept_codes:
        master_dept_codes.append("ungrouped")

    if args.department:
        filtered_sheet_order = list(args.department)
        filtered_all_sheets = {
            code: all_sheets[code]
            for code in filtered_sheet_order
            if code in all_sheets
        }
        filtered_summary_df = summary_df[
            summary_df["Department"].isin(
                [DISPLAY_MAP.get(code, code) for code in filtered_sheet_order]
            )
        ]
    else:
        filtered_sheet_order = master_dept_codes
        filtered_all_sheets = all_sheets
        filtered_summary_df = summary_df

    # Write master and department reports
    write_full_report(
        all_sheets,
        summary_df, # type: ignore
        master_dept_codes,
        args.output_path,
        include_ungrouped=True,
    )
    dept_summaries: List[tuple[str, str]] = []
    if not args.master_only:
        output_directory = os.path.dirname(args.output_path) or os.getcwd()

        # Ensure filtered_summary_df is a proper DataFrame, not a single Series
        if isinstance(filtered_summary_df, pd.Series):
            filtered_summary_df = filtered_summary_df.to_frame().T

        dept_summaries = (
            write_department_reports(
                filtered_all_sheets,
                filtered_summary_df, # type: ignore
                filtered_sheet_order,
                output_directory,
                include_ungrouped=(args.department is None),
            )
            or []
        )
    else:
        logger.info("Skipping individual department reports (--master-only)")

    # Email section
    if args.send_emails:
        try:

            # Ensure path resolution for PyInstaller and fallback if needed
            config_file = args.emails_config
            if not os.path.isfile(config_file):
                config_file = resource_path(os.path.basename(config_file))
            if not os.path.isfile(config_file):
                logger.error("Email mapping not found at: %s", args.emails_config)
                sys.exit(1)

            # Load email recipient config
            with open(config_file, encoding="utf-8") as f:
                email_map = json.load(f)

            # Send one email per department
            for dept_code, report_path in dept_summaries:
                recipients = email_map.get(dept_code)
                if not recipients:
                    logger.warning("No recipients for '%s'; skipping email", dept_code)
                    continue

                subject = f"Microsoft Defender Report for {dept_code} – {reference_date.isoformat()}"
                body = (
                    f"Please find attached the Microsoft Defender report for department {dept_code} "
                    f"generated on {reference_date.isoformat()}.\n\n"
                    f"Regards,\nAV Team"
                )

                try:
                    send_email(
                        smtp_server=args.smtp_server,
                        smtp_port=args.smtp_port,
                        from_addr=args.from_email,
                        to_addrs=recipients,
                        subject=subject,
                        body=body,
                        attachments=[report_path],
                        smtp_user=getattr(args, "smtp_user", None),
                        smtp_password=getattr(args, "smtp_password", None),
                    )
                    logger.info("Email sent to %s for '%s'", recipients, dept_code)
                except Exception as e:
                    logger.error("Failed to send email for '%s': %s", dept_code, e)

        except Exception as e:
            logger.exception("Unexpected error during email sending: %s", e)
    else:
        logger.info("Email sending disabled (--no-emails)")

    # Print summary table
    try:
        from tabulate import tabulate
    except ImportError:
        logger.warning(
            "Install tabulate (`pip install tabulate`) to see a summary table"
        )
    else:
        print()
        print("✓ Reports complete:\n")
        table = [("Master", args.output_path)] + dept_summaries
        print(tabulate(table, headers=["Report", "Path"], tablefmt="github"))
        print()

    if os.name == "nt":
        width = shutil.get_terminal_size((80, 20)).columns
        msg = "✅  Reports complete  ✅"
        border = "=" * width
        print()
        print(border)
        print(msg.center(width))
        print(border)
        print()
        input("Press ENTER to close this window…")


if __name__ == "__main__":
    main()
