import os
import re
import sys
from typing import Dict, List, Optional

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Map official department codes → sheet names
DEPARTMENT_CODE_TO_SHEET: Dict[str, str] = {
    "GPEDU": "gpedu",
    "GPHEALTH": "gphealth",
    "GPGDED": "gpgded",
    "GDSD": "gdsd",
    "GPSPORTS": "gpsports",
    "GDARD": "gdard",
    "GPT": "gpt",
    "GPDRT": "gpdrt",
    "GPEGOV": "gpegov",
    "GPDID": "gpdid",
    "GDHUS": "gdhus",
    "GPDPR": "gpdpr",
    "GPSAS": "gpsas",
    "COGTA": "cogta",
    "GIFA": "gifa",
}

# Common text variants → official code
VARIANT_TO_CODE: Dict[str, str] = {
    "gpdrt - k": "GPDRT",
    "gdrt": "GPDRT",
    "gp health": "GPHEALTH",
    "bgh": "GPHEALTH",
    # …etc…
}

def extract_bracket_text(user_name: str) -> Optional[str]:
    """
    Extract the text inside the final parentheses of `user_name`.
    E.g. "Bob (GPEDU)" → "gpedu"
    """
    match = re.search(r"\(([^)]+)\)\s*$", user_name.strip())
    if not match:
        return None
    return match.group(1).strip().lower()

def normalize_department_code(raw_text: Optional[str]) -> Optional[str]:
    """
    Convert raw bracket text to one of the official uppercase codes.
    Returns None if it cannot be normalized.
    """
    if not raw_text:
        return None
    if raw_text in VARIANT_TO_CODE:
        return VARIANT_TO_CODE[raw_text]
    cleaned = re.sub(r"[^A-Za-z0-9]", "", raw_text).upper()
    return cleaned if cleaned in DEPARTMENT_CODE_TO_SHEET else None

def load_sheet_order(template_path: str) -> List[str]:
    """
    Return the sheet names (in their original order) from the AVReport.xlsx template.
    Exits with an error if the template is missing.
    """
    if not os.path.exists(template_path):
        logger.error("Template not found: %s", template_path)
        sys.exit(1)
    return pd.ExcelFile(template_path, engine="openpyxl").sheet_names

def group_rows_by_department(data_frame: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Group each row of `data_frame` into a dict of sheet_name → DataFrame,
    based on the department code extracted from the UserName column.
    Rows with no valid code go under the "ungrouped" key.
    """
    groups: Dict[str, List[pd.Series]] = {}
    for _, row in data_frame.iterrows():
        raw = extract_bracket_text(str(row.get("UserName", "")))
        code = normalize_department_code(raw)
        sheet_name = DEPARTMENT_CODE_TO_SHEET.get(code, "ungrouped")
        groups.setdefault(sheet_name, []).append(row)
    return {name: pd.DataFrame(rows) for name, rows in groups.items()}
