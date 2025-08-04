# utils.py

import itertools
import logging
import os
import re
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from typing import List, Optional, Tuple
from functools import lru_cache
import requests
import pandas as pd
import datetime

CACHE_EXPIRY = 30 * 60  # 30 minutes


@lru_cache()
def _fetch_url(url):
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.text


def fetch_live_defender_versions(use_cache=True):
    """
    Returns dict with keys: signature (latest sig version/date),
    engine, platform, with caching & error handling.
    """
    now = time.time()
    cache = fetch_live_defender_versions.cache = getattr(
        fetch_live_defender_versions, "cache", {}
    )
    if use_cache and "ts" in cache and now - cache["ts"] < CACHE_EXPIRY:
        return cache["data"]

    data = {}
    try:
        text = _fetch_url("https://www.microsoft.com/en-us/wdsi/defenderupdates")
        engine_match = re.search(r"Engine Version:\s*<span>([\d\.]+)</span>", text)
        if engine_match:
            data["engine"] = engine_match.group(1)
        platform_match = re.search(r"Platform Version:\s*<span>([\d\.]+)</span>", text)
        if platform_match:
            data["platform"] = platform_match.group(1)
    except Exception as e:
        logging.warning("Failed to fetch engine/platform: %s", e)

    try:
        text = _fetch_url(
            "https://www.microsoft.com/en-us/wdsi/definitions/antimalware-definition-release-notes"
        )
        # grab first drop down entry
        m = re.search(r"dropDownOption[^>]*>([\d\.]+)</", text)
        if m:
            ver = m.group(1)
            # now fetch release date:
            txt2 = _fetch_url(
                f"https://www.microsoft.com/en-us/wdsi/definitions/antimalware-definition-release-notes?requestVersion={ver}"
            )
            dtm_match = re.search(r"releaseDate_0[^>]*>([^<]+)</", txt2)
            if dtm_match:
                dtm = dtm_match.group(1)
                data["signature"] = ver
                data["released"] = datetime.datetime.strptime(dtm, "%m/%d/%Y %I:%M:%S %p")
    except Exception as e:
        logging.warning("Failed to fetch signature version/date: %s", e)

    cache["ts"] = now
    cache["data"] = data
    return data

def resource_path(relative_path: str) -> str:
    base = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))
    return os.path.join(base, relative_path)


def extract_date_from_filename(filename: str) -> str:
    """Extract an ISO date from the filename, or prompt if not found."""
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return match.group(1)
    return input(f"Enter report date for {filename} [YYYY-MM-DD]: ")


def print_summary_table(master_path: str, dept_summaries: List[tuple]) -> None:
    """Pretty-print a summary table using tabulate (if available)."""
    try:
        from tabulate import tabulate
    except ImportError:
        print("(Install tabulate to see summary tables)")
        return
    print()
    print("✓ Reports complete:\n")
    table = [("Master", master_path)] + dept_summaries
    print(tabulate(table, headers=["Report", "Path"], tablefmt="github"))
    print()


def validate_departments(
    user_departments: List[str], valid_departments: List[str]
) -> List[str]:
    """Returns a list of any department codes in user_departments not found in valid_departments."""
    return [
        dept
        for dept in user_departments
        if dept not in valid_departments and dept != "ungrouped"
    ]


def configure_logging(
    log_file_path: Optional[str] = None, level: int = logging.INFO
) -> None:
    """
    Configure root logger to:
      • Stream to STDOUT
      • Optionally write to a rotating file
    """
    root_logger = logging.getLogger()
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    root_logger.setLevel(level)
    fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    if log_file_path:
        log_dir = os.path.dirname(log_file_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


class Spinner:
    """
    Context manager that displays a terminal spinner while a long-running
    operation is in progress.
    """

    def __init__(self, message: str = "Working"):
        self.message = message
        self._spinner_cycle = itertools.cycle(["|", "/", "-", "\\"])
        self._stop_event = threading.Event()

    def __enter__(self) -> "Spinner":
        threading.Thread(target=self._spin, daemon=True).start()
        return self

    def _spin(self) -> None:
        while not self._stop_event.is_set():
            sys.stdout.write(f"\r{self.message} {next(self._spinner_cycle)}")
            sys.stdout.flush()
            time.sleep(0.1)

    def __exit__(self, *args) -> None:
        self._stop_event.set()
        sys.stdout.write("\r" + " " * (len(self.message) + 2) + "\r")
        sys.stdout.flush()


def parse_version(version: str) -> Tuple[int, ...]:
    """
    Parse a Defender version string (e.g., "1.1.25060.6") into a tuple of integers.
    Handles missing or malformed versions robustly.
    """
    if not version or not isinstance(version, str):
        return tuple()
    return tuple(int(x) for x in re.findall(r"\d+", version))


def version_n_minus(version: str, n: int) -> str:
    """
    Subtract n from the major version number (first segment) and return as a string.
    e.g., version_n_minus("1.1.25060.6", 2) -> "0.1.25060.6"
    """
    parts = list(parse_version(version))
    if parts:
        parts[0] = max(0, parts[0] - n)
    return ".".join(str(x) for x in parts)

def make_datetime_columns_timezone_naive(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert any timezone-aware datetime columns in a DataFrame to timezone-naive.
    This is required for Excel export (Excel does not support tz-aware datetimes).
    """
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            if hasattr(df[col].dt, "tz") and df[col].dt.tz is not None:
                df[col] = df[col].dt.tz_localize(None)
        elif (
            df[col]
            .apply(lambda x: isinstance(x, datetime.datetime) and x.tzinfo is not None)
            .any()
        ):
            # For object columns with tz-aware Python datetimes
            df[col] = df[col].apply(
                lambda x: x.replace(tzinfo=None)
                if isinstance(x, datetime.datetime) and x.tzinfo
                else x
            )
    return df
