import os
import re
import sys
from typing import Dict, List, Optional

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Official department code to canonical (sheet) name mapping
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

# Text variant → official code mapping (expand as needed)
VARIANT_TO_CODE: Dict[str, str] = {
    # GPDRT Variants
    "gpdrt - k": "GPDRT",
    "gdrt": "GPDRT",
    "(DLTC)": "GPDRT",
    "(GFLEET)": "GPDRT",
    "(GPDTR)": "GPDRT",
    "(GPDRT": "GPDRT",
    "(GPDRT))": "GPDRT",
    "(GPDRT-K)": "GPDRT",

    # GPHEALTH Variants
    "gp health": "GPHEALTH",
    "(GPHEALTH))": "GPHEALTH",
    "(GPHEALTH": "GPHEALTH",
    "(GPHEALTH0": "GPHEALTH",
    "(gpghealth)": "GPHEALTH",
    "(gphealth": "GPHEALTH",
    "(gphealth0": "GPHEALTH",
    "Gphealth)": "GPHEALTH",
    "(GPHealth": "GPHEALTH",
    "(GPHeallth)": "GPHEALTH",
    "(GPHealth}": "GPHEALTH",
    "(GpHealth": "GPHEALTH",
    "bgh": "GPHEALTH",
    "(GHealth)": "GPHEALTH",
    "(GPHEALH)": "GPHEALTH",
    "(GPGEALTH)": "GPHEALTH",
    "(gphiealth)": "GPHEALTH",

    # GPEGOV Variants
    "egov": "GPEGOV",
    "EGOV": "GPEGOV",

    # GPDID Variants
    "DID": "GPDID",
    "(DID))": "GPDID",
    "(GDID)": "GPDID",

    # GPEDU Variants
    "GPEDU)": "GPEDU",
    "(GPEDU))": "GPEDU",
    "(GDE)": "GPEDU",
    "(GDEDU)": "GPEDU",
    "(GDEDU)2": "GPEDU",
    "(GPEDU)1": "GPEDU",
    "[GPEDU]": "GPEDU",
    "(GPED)": "GPEDU",
    "(MC)": "GPEDU",
    "(PEDU)": "GPEDU",

    # GDARD Variants
    "(GDACE)": "GDARD",
    "(GDARDE)": "GDARD",
    "(GDARD": "GDARD",
    "(GDEnv)": "GDARD",

    # GPGDED Variants
    "(GPDED)": "GPGDED",
    "(GPGDED": "GPGDED",
    "GPGDED": "GPGDED",

    # GPDPR Variants
    "(GPRPR)": "GPDPR",
    "GPDPR)": "GPDPR",
    "(GPDPDR)": "GPDPR",

    # GDHUS Variants
    "(GDHuS": "GDHUS",
    "(GDHS)": "GDHUS",

    # GPSAS Variants
    "GPSAS)": "GPSAS",
    "(GPSAS))": "GPSAS",

    # GPSPORTS Variants
    "(GPsport)": "GPSPORTS",

    # GIFA
    "gifa": "GIFA",
}


def extract_bracket_text(user_name: str) -> Optional[str]:
    """
    Extract the text inside the final parentheses of `user_name`.
    E.g. "Bob (GPEDU)" → "GPEDU"
    """
    if not user_name:
        return None
    match = re.search(r"\(([^)]+)\)\s*$", user_name.strip())
    if not match:
        return None
    return match.group(1).strip()

def normalize_department_code(raw_text: Optional[str]) -> Optional[str]:
    """
    Convert raw bracket text to one of the official uppercase codes.
    Returns None if it cannot be normalized.
    """
    if not raw_text:
        return None
    lower_raw = raw_text.lower().strip()
    if lower_raw in VARIANT_TO_CODE:
        return VARIANT_TO_CODE[lower_raw]
    cleaned = re.sub(r"[^A-Za-z0-9]", "", raw_text).upper()
    if cleaned in DEPARTMENT_CODE_TO_SHEET:
        return cleaned
    return None

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
        user_name = str(row.get("UserName", "")).strip()
        raw = extract_bracket_text(user_name)
        code = normalize_department_code(raw)
        sheet_name = DEPARTMENT_CODE_TO_SHEET.get(code, "ungrouped")
        groups.setdefault(sheet_name, []).append(row)
    return {name: pd.DataFrame(rows) for name, rows in groups.items()}
