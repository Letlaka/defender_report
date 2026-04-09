#!/usr/bin/env python3
"""
Command-line entry point for the DefenderAgents report generator.
"""

import argparse
import datetime
import json
import logging
import os
import pathlib
import re
import shutil
import sys
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv

from defender_report.categorization import categorize_dataframe, tally_dataframe
from defender_report.emailer import send_email
from defender_report.grouping import group_rows_by_device_prefix, load_sheet_order
from defender_report.reporting import write_department_reports, write_full_report
from defender_report.utils import Spinner, configure_logging

logger = logging.getLogger(__name__)


# Environment setup
def resource_path(relative_path: str) -> str:
    """Get absolute path to resource (handles PyInstaller _MEIPASS)."""
    base = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base, relative_path)


def load_env():
    # 1) .env next to the EXE, 2) CWD .env, 3) bundled .env
    candidates = []
    if getattr(sys, "frozen", False):
        candidates.append(os.path.join(os.path.dirname(sys.executable), ".env"))
    candidates += [
        os.path.join(os.getcwd(), ".env"),
        resource_path(".env"),
    ]
    for p in candidates:
        if os.path.exists(p):
            load_dotenv(p, override=True)
            logger.info("Loaded .env from %s", p)
            return
    logger.warning(".env not found; relying on process env only.")


load_env()

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
    "environment": "Environment",
    "ungrouped": "ungrouped",
}


class CustomFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter
):
    pass


def parse_command_line_arguments():
    parser = argparse.ArgumentParser(
        description="Command-line entry point for the DefenderAgents report generator.",
        formatter_class=CustomFormatter,
    )

    # Core I/O
    core = parser.add_argument_group("Core I/O")
    core.add_argument(
        "--input-path",
        default="DefenderAgents.xlsx",
        help="Path to input Excel file with Defender agent data.",
    )
    core.add_argument(
        "--template-path",
        default="AVReport.xlsx",
        help="Path to Excel template for sheet order.",
    )
    core.add_argument(
        "--output-path",
        default="DefenderAgents_Report.xlsx",
        help="Path for the generated master report Excel file.",
    )

    # AD enrichment
    enrich = parser.add_argument_group("AD enrichment")
    enrich.add_argument(
        "--enrich-ad",
        action="store_true",
        help="Enable Active Directory enrichment if available.",
    )
    enrich.add_argument(
        "--use-cache",
        action="store_true",
        help="Use cached AD enrichment results if available.",
    )
    enrich.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear cached AD enrichment results before querying.",
    )
    enrich.add_argument(
        "--export-unmatched-dir",
        type=str,
        help="Directory to save unmatched devices as CSV or JSON.",
    )

    # Processing options
    processing = parser.add_argument_group("Processing options")
    processing.add_argument(
        "--date",
        default=datetime.date.today().isoformat(),
        help="Reference date for compliance checks (YYYY-MM-DD).",
    )
    processing.add_argument(
        "--threshold-days",
        type=int,
        default=7,
        help="Number of days since last seen before marking device as 'Out of Date'.",
    )
    processing.add_argument(
        "--department",
        nargs="+",
        metavar="DEPT",
        help="Filter to specific department code(s) (space-separated).",
    )
    processing.add_argument(
        "--master-only",
        action="store_true",
        help="Only generate the master summary report (skip per-department reports).",
    )

    # Emailing
    emailing = parser.add_argument_group("Emailing")
    emailing.add_argument(
        "--emails-config",
        default=os.getenv("EMAILS_CONFIG", resource_path("emails_config.json")),
        help="Path to recipients mapping JSON (dept_code -> [emails]).",
    )
    # Mutually exclusive: --send-emails / --no-emails
    mx = emailing.add_mutually_exclusive_group()
    mx.add_argument(
        "--send-emails",
        action="store_true",
        default=os.getenv("SEND_EMAILS", "false").lower() in ("1", "true", "yes"),
        help="Send department emails with attachments.",
    )
    mx.add_argument(
        "--no-emails",
        dest="send_emails",
        action="store_false",
        help="Do not send emails (default unless SEND_EMAILS=true).",
    )

    emailing.add_argument(
        "--smtp-server",
        default=os.getenv("SMTP_HOST") or os.getenv("SMTP_SERVER"),
        help="SMTP server hostname.",
    )
    emailing.add_argument(
        "--smtp-port",
        type=int,
        default=int(os.getenv("SMTP_PORT", "587")),
        help="SMTP port (587 for STARTTLS, 25 or 2525 for plain relay).",
    )
    emailing.add_argument(
        "--smtp-user",
        default=os.getenv("SMTP_USERNAME") or os.getenv("SMTP_USER"),
        help="SMTP username (optional for relay).",
    )
    emailing.add_argument(
        "--smtp-password",
        default=os.getenv("SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD"),
        help="SMTP password (optional for relay).",
    )
    emailing.add_argument(
        "--from-email",
        default=os.getenv("FROM_ADDRESS") or os.getenv("FROM_EMAIL"),
        help="From address used when sending emails.",
    )
    emailing.add_argument(
        "--cc-email",
        default=os.getenv("CC_EMAIL", ""),
        help="Comma-separated CC list (optional).",
    )

    # Debug & runtime behavior
    debug = parser.add_argument_group("Debug & runtime behavior")
    debug.add_argument(
        "--log-file",
        default=None,
        help="Optional path for log file (default: log only to console).",
    )
    debug.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing reports or sending emails (for testing/debug).",
    )
    debug.add_argument(
        "--verbose", action="store_true", help="Enable verbose debug logging."
    )
    debug.add_argument(
        "--resource-root",
        type=str,
        help="Override directory for static resources like .env and template.",
    )
    debug.add_argument(
        "--open-output",
        action="store_true",
        help="Open the report folder after completion (Windows only).",
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

    with Spinner(f"Reading {os.path.basename(args.input_path)}"):
        ext = pathlib.Path(args.input_path).suffix.lower()
        try:
            # Prefer openpyxl for modern xlsx files (already declared in pyproject)
            if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
                data_frame = pd.read_excel(args.input_path, engine="openpyxl")
            elif ext == ".xls":
                # .xls requires xlrd; let pandas pick the engine so the original
                # ImportError is raised if xlrd is missing and we can show a helpful
                # message below.
                data_frame = pd.read_excel(args.input_path)
            else:
                data_frame = pd.read_excel(args.input_path)
        except ImportError as e:
            msg = str(e)
            if "xlrd" in msg:
                logger.error(
                    "Missing dependency 'xlrd' required for .xls files.\n"
                    "Convert the input to .xlsx (recommended) or install xlrd>=2.0.1 "
                    "and rebuild the EXE.\nSee: pip install xlrd\n"
                )
            else:
                logger.exception("Failed to read Excel file: %s", e)
            sys.exit(1)
    logger.info("Loaded %d rows from %s", len(data_frame), args.input_path)

    # Remove rows with missing or blank UserName/DeviceName
    if "UserName" in data_frame.columns and "DeviceName" in data_frame.columns:
        before_count = len(data_frame)

        data_frame = data_frame.dropna(subset=["UserName", "DeviceName"], how="all")

        data_frame = data_frame[
            (data_frame["DeviceName"].astype(str).str.strip() != "")
            | (data_frame["UserName"].astype(str).str.strip() != "")
        ]

        logger.info(
            "Filtered out %d rows where BOTH DeviceName and UserName were empty",
            before_count - len(data_frame),
        )
    else:
        logger.error("'DeviceName' or 'UserName' column is missing from input data.")
        sys.exit(1)

    logger.info("Loaded %d valid rows from %s", len(data_frame), args.input_path)

    if "UserName" not in data_frame.columns:
        logger.warning(
            "Input does not contain 'UserName' column. Device-based grouping will be used exclusively."
        )

    # Group by department and load template order
    grouped_sheets = group_rows_by_device_prefix(data_frame)

    # Ensure template path resolves for PyInstaller
    if not os.path.isfile(args.template_path):
        alt = resource_path(os.path.basename(args.template_path))
        if os.path.isfile(alt):
            logger.info("Using bundled template: %s", alt)
            args.template_path = alt
        else:
            logger.error("Template not found: %s", args.template_path)
            sys.exit(1)

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

    # Resolve which departments we will process
    if args.department:
        departments = list(args.department)
    else:
        departments = list(sheet_order)
        if "ungrouped" not in departments:
            departments.append("ungrouped")

    # Per-department logic
    all_sheets: Dict[str, pd.DataFrame] = {
        dept: grouped_sheets.get(dept, pd.DataFrame(columns=data_frame.columns))
        for dept in departments
    }
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
            tally["Department"] = dept_code  # type: ignore
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
        logger.warning(
            "Missing expected summary columns: %s", ", ".join(missing_columns)
        )
        for col in missing_columns:
            summary_df[col] = None  # Fill missing ones with None

    summary_df = summary_df[expected_summary_columns]
    summary_df.rename(columns={"Intune": "Intune Managed"}, inplace=True)  # type: ignore

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

    # Also include any discovered sheets that are not present in the template
    extra_from_data = [s for s in all_sheets.keys() if s not in master_dept_codes]
    if extra_from_data:
        master_dept_codes.extend(sorted(extra_from_data))
    # Ensure 'environment' specifically is present when discovered
    if "environment" in all_sheets and "environment" not in master_dept_codes:
        master_dept_codes.append("environment")

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
        summary_df,  # type: ignore
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
                filtered_summary_df,  # type: ignore
                filtered_sheet_order,
                output_directory,
                include_ungrouped=(args.department is None),
            )
            or []
        )
    else:
        logger.info("Skipping individual department reports (--master-only)")

    # Email section
    # Email section
    if args.send_emails:
        try:
            # Resolve recipients JSON path (handles PyInstaller bundle)
            config_file = args.emails_config
            if not os.path.isfile(config_file):
                config_file = resource_path(os.path.basename(config_file))
            if not os.path.isfile(config_file):
                logger.error("Email mapping not found at: %s", args.emails_config)
                sys.exit(1)

            with open(config_file, encoding="utf-8") as f:
                email_map = json.load(f)
                # normalize mapping keys to lower-case so dept codes (e.g. 'gpdid') match
                email_map = {k.lower(): v for k, v in email_map.items()}
                # Also map DISPLAY_MAP values back to their department codes
                # e.g. JSON may use "EDUCATION" but sheet codes are 'gpedu'
                for dept_code, display_name in DISPLAY_MAP.items():
                    display_key = display_name.lower()
                    if display_key in email_map and dept_code not in email_map:
                        email_map[dept_code] = email_map[display_key]

            # Guard SMTP essentials so we don't call SMTP(None, 587)
            if not args.smtp_server:
                logger.error(
                    "SMTP server not set. Use --smtp-server or SMTP_HOST/SMTP_SERVER in .env"
                )
                sys.exit(1)
            if not args.from_email:
                logger.error(
                    "From address not set. Use --from-email or FROM_ADDRESS/EMAIL_FROM in .env"
                )
                sys.exit(1)

            # Light masking for logs
            def _mask_user(u: str | None) -> str:
                if not u:
                    return ""
                return (u[:2] + "***") if len(u) > 3 else "***"

            logger.info(
                "Email config -> host=%s port=%s user=%s from=%s map=%s",
                args.smtp_server,
                args.smtp_port,
                _mask_user(args.smtp_user),
                args.from_email,
                config_file,
            )

            # Normalize dept key for mapping (handles e.g. gpgpedu vs gpedu)
            def normalize_dept(code: str) -> str:
                c = code.lower().strip()
                if c in email_map:
                    return c
                # common variant: gpgxxxx -> gpxxxx
                if c.startswith("gpg") and ("gp" + c[3:]) in email_map:
                    return "gp" + c[3:]
                # remove non-letters as last resort
                c2 = re.sub(r"[^a-z]", "", c)
                return c2 if c2 in email_map else c

            # Send one email per department report
            for dept_code, report_path in dept_summaries:
                key = normalize_dept(dept_code)
                recipients = email_map.get(key)
                if not recipients:
                    logger.warning(
                        "No recipients for '%s' (normalized '%s'); skipping email",
                        dept_code,
                        key,
                    )
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
                        cc_addrs=args.cc_email,
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
