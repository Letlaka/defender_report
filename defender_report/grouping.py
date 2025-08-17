import difflib
import logging
import os
import re
import sys
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ----------------------------
# Canonical department → sheet
# ----------------------------
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

# -----------------------------------------
# User-entered text variants → official code
# (Keep adding raw variants here if needed;
#  the normalization below makes them robust)
# -----------------------------------------
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
    "(GPDRT- K)": "GPDRT",
    # GPHEALTH Variants
    "gp health": "GPHEALTH",
    "(GPHEALTH))": "GPHEALTH",
    "(GPHEALTH": "GPHEALTH",
    "(GPHEALTH0": "GPHEALTH",
    "(gpghealth)": "GPHEALTH",
    "(gphealth": "GPHEALTH",
    "(gphealth0)": "GPHEALTH",
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

# --------------------------------------
# Device name → official department code
# --------------------------------------
DEVICE_PREFIX_MAP: Dict[str, str] = {
    "GPGDED": "GPGDED",
    # 5-character rules checked first
    "GDHUS": "GDHUS",
    # 4-character rules
    "SACR": "GPSPORTS",
    "GIFA": "GIFA",
    # 3-character rules
    "AGR": "GDARD",
    "OOP": "GPDPR",
    "AEG": "GPEGOV",
    "DRT": "GPDRT",
    "GFB": "GPDRT",
    "GDE": "GPEDU",
    "DED": "GPGDED",
    "DID": "GPDID",
    "DCS": "GPSAS",
    "ATR": "GPT",
    "COG": "COGTA",
    "GDS": "GDSD",
}
# Special case: GDH (but not GDHUS) → GPHEALTH
_GDH_PATTERN = re.compile(r"^GDH(?!US)")

# ------------------------
# Normalization helpers
# ------------------------
_NON_ALNUM = re.compile(r"[^a-z0-9]")
_TOKEN_SPLIT = re.compile(r"[^A-Za-z0-9]+")


def _canon(s: str) -> str:
    """lowercase and strip non-alphanumerics for robust matching."""
    return _NON_ALNUM.sub("", s.lower())


# Build normalized maps once
_VARIANT_LOWER: Dict[str, str] = {k.lower(): v for k, v in VARIANT_TO_CODE.items()}
_VARIANT_CANON: Dict[str, str] = {_canon(k): v for k, v in VARIANT_TO_CODE.items()}

# Also allow official codes to match in cleaned form
_OFFICIAL_CODES = list(DEPARTMENT_CODE_TO_SHEET.keys())
_OFFICIAL_CANON: Dict[str, str] = {
    _canon(k): k for k in DEPARTMENT_CODE_TO_SHEET.keys()
}

def _closest_official(cleaned_upper: str, cutoff: float = 0.8) -> Optional[str]:
    """Fuzzy match to nearest official code (handles GPHEATH→GPHEALTH, PGDID→GPDID)."""
    matches = difflib.get_close_matches(
        cleaned_upper, _OFFICIAL_CODES, n=1, cutoff=cutoff
    )
    return matches[0] if matches else None

# ------------------------
# Extraction + normalization
# ------------------------
def extract_bracket_text(user_name: str) -> Optional[str]:
    """
    Extract the text inside the final parentheses of `user_name`.
    E.g. "Bob (GPEDU)" → "GPEDU"
    """
    if not user_name:
        return None
    match = re.search(r"\(([^)]+)\)\s*$", str(user_name).strip())
    return match.group(1).strip() if match else None


def _normalize_variant(raw_text: str) -> Optional[str]:
    """
    Normalize using variant maps (case & punctuation tolerant),
    then fall back to official codes after cleaning.
    """
    lower_raw = raw_text.lower().strip()
    cleaned_lower = _canon(lower_raw)
    cleaned_upper = cleaned_lower.upper()

    # 1) exact-lower variant key
    if lower_raw in _VARIANT_LOWER:
        return _VARIANT_LOWER[lower_raw]

    # 2) cleaned variant key (paren/space insensitive)
    if cleaned_lower in _VARIANT_CANON:
        return _VARIANT_CANON[cleaned_lower]

    # 3) cleaned official code
    if cleaned_upper in DEPARTMENT_CODE_TO_SHEET:
        return cleaned_upper

    # 4) cleaned official via canon map
    if cleaned_lower in _OFFICIAL_CANON:
        return _OFFICIAL_CANON[cleaned_lower]

    # fuzzy official
    fuzzy = _closest_official(cleaned_upper)
    return fuzzy

def normalize_department_code(raw_text: Optional[str]) -> Optional[str]:
    """Public normalizer wrapper (kept for compatibility)."""
    if not raw_text:
        return None
    code = _normalize_variant(raw_text)
    if code is None:
        logger.debug("Unrecognized dept text: %r", raw_text)
    return code


def _scan_username_for_code(user_name: str) -> Optional[str]:
    """
    If bracket extraction fails, scan tokens AND substrings of the cleaned username
    for official codes or near-misses.
    """
    if not user_name:
        return None
    # 1) token-wise (tries longer tokens first)
    tokens = [t for t in _TOKEN_SPLIT.split(str(user_name)) if t]
    for tok in sorted(tokens, key=len, reverse=True):
        norm = _normalize_variant(tok)
        if norm:
            return norm

    # 2) substring scan on the cleaned string (handles glued codes like '...MantingeGPGDED)')
    cleaned = _canon(user_name)
    # exact substring of any official code (cleaned)
    for code in _OFFICIAL_CODES:
        if _canon(code) in cleaned:
            return code
    # fuzzy: probe every 5–7 length window (lengths of our codes) for nearest official
    for L in (5, 6, 7, 8):  # safe window sizes
        for i in range(0, max(0, len(cleaned) - L + 1)):
            candidate = cleaned[i : i + L].upper()
            fuzzy = _closest_official(candidate, cutoff=0.82)
            if fuzzy:
                return fuzzy
    return None


# ------------------------
# Device name heuristics
# ------------------------
def department_from_device_name(device_name: str) -> Optional[str]:
    """Infer department code based on device name prefix and mapping rules."""
    if not device_name or not isinstance(device_name, str):
        return None
    dn = device_name.strip().upper()

    # 5-char prefix first
    if dn.startswith("GDHUS"):
        return "GDHUS"
    if dn.startswith("SACRT"):
        return "GPSPORTS"

    # GDH but not GDHUS → GPHEALTH
    if _GDH_PATTERN.match(dn):
        return "GPHEALTH"

    # 3-char map
    for pref, code in DEVICE_PREFIX_MAP.items():
        if dn.startswith(pref):
            return code
    return None


# ------------------------
# Sheet/template helpers
# ------------------------
def load_sheet_order(template_path: str) -> List[str]:
    """
    Return the sheet names (in their original order) from the AVReport.xlsx template.
    Exits with an error if the template is missing.
    """
    if not os.path.exists(template_path):
        logger.error("Template not found: %s", template_path)
        sys.exit(1)
    return [
        str(name) for name in pd.ExcelFile(template_path, engine="openpyxl").sheet_names
    ]


# ------------------------
# Main grouping
# ------------------------
def get_department_code(user_name: str, device_name: str) -> Optional[str]:
    """
    Resolve a department code using:
      1) final bracket text from username,
      2) token scan of username,
      3) device name prefix rules.
    """
    # 1) Bracket text
    raw = extract_bracket_text(user_name)
    code = normalize_department_code(raw) if raw else None
    if code:
        return code

    # 2) Token scan of the whole username
    code = _scan_username_for_code(user_name)
    if code:
        return code

    # 3) Device prefix fallback
    code = department_from_device_name(device_name)
    if code:
        return code

    return None


def group_rows_by_department(data_frame: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Group each row of `data_frame` into a dict of sheet_name → DataFrame.
    Order of detection: username (brackets) → username token scan → device prefix.
    Rows with no valid code go under "ungrouped".
    """
    groups: Dict[str, List[pd.Series]] = {}
    for _, row in data_frame.iterrows():
        user_name = str(row.get("UserName", "") or "").strip()
        device_name = str(row.get("DeviceName", "") or "").strip()

        code = get_department_code(user_name, device_name)
        sheet_name = DEPARTMENT_CODE_TO_SHEET.get(code or "", "ungrouped")
        groups.setdefault(sheet_name, []).append(row)

        if code is None:
            logger.debug("Ungrouped row — user=%r device=%r", user_name, device_name)

    return {name: pd.DataFrame(rows) for name, rows in groups.items()}


def group_rows_by_device_prefix(data_frame: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Group rows strictly by DeviceName prefix using department_from_device_name().
    Rows that don't match any known department code are placed in 'ungrouped'.
    """
    grouped: Dict[str, list] = {
        sheet: [] for sheet in DEPARTMENT_CODE_TO_SHEET.values()
    }
    grouped["ungrouped"] = []

    if "DeviceName" not in data_frame.columns:
        # If DeviceName column missing, everything is 'ungrouped'
        grouped["ungrouped"] = data_frame.to_dict("records")
        return {k: pd.DataFrame(v) for k, v in grouped.items()}

    for _, row in data_frame.iterrows():
        device_name = str(row["DeviceName"])
        dept_code = department_from_device_name(device_name)
        if dept_code:
            sheet_name = DEPARTMENT_CODE_TO_SHEET.get(dept_code, "ungrouped")
        else:
            sheet_name = "ungrouped"

        grouped[sheet_name].append(row)

    # Convert to DataFrames
    return {sheet: pd.DataFrame(rows) for sheet, rows in grouped.items()}