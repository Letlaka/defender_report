# defender_report/unmatched_utils.py
import os
import re
import json
import logging
from typing import Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

PREFIX_RE = re.compile(r"^[A-Z]{3,5}")


def _safe_lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _guess_from_variants(
    text: str, variant_to_code: Dict[str, str]
) -> Optional[Tuple[str, str]]:
    """
    Try a contains/equals match on known variants/aliases (case-insensitive).
    """
    if not text:
        return None
    low = _safe_lower(text)
    for variant, code in variant_to_code.items():
        v = variant.strip().lower()
        if v == low or v in low:
            return code, f"variant:{variant}"
    return None


def _guess_from_prefix(
    device_name: str, device_prefix_map: Dict[str, str]
) -> Optional[Tuple[str, str]]:
    """
    Try 5, then 4, then 3-char prefixes from the start of DeviceName.
    """
    if not device_name:
        return None
    upper = device_name.upper().strip()
    for n in (5, 4, 3):
        if len(upper) >= n:
            p = upper[:n]
            if p in device_prefix_map:
                return device_prefix_map[p], f"prefix:{p}"
    # regex prefix fallback (usually redundant with above)
    m = PREFIX_RE.match(upper)
    if m and m.group(0) in device_prefix_map:
        p = m.group(0)
        return device_prefix_map[p], f"prefix:{p}"
    return None


def _row_guess(
    device_name: str, device_prefix_map: Dict[str, str], variant_to_code: Dict[str, str]
) -> Tuple[Optional[str], Optional[str]]:
    # 1) Prefix map
    g = _guess_from_prefix(device_name, device_prefix_map)
    if g:
        return g
    # 2) Variants on the raw name (helps for odd namings)
    g = _guess_from_variants(device_name, variant_to_code)
    if g:
        return g
    return None, None


def classify_unmatched_df(
    unmatched_df: pd.DataFrame,
    device_prefix_map: Dict[str, str],
    variant_to_code: Dict[str, str],
) -> pd.DataFrame:
    """
    Input: DataFrame with at least a 'DeviceName' column.
    Output: same rows + DepartmentGuess + GuessReason.
    """
    if unmatched_df.empty:
        return unmatched_df.assign(
            DepartmentGuess=pd.Series(dtype="string"),
            GuessReason=pd.Series(dtype="string"),
        )

    guesses = (
        unmatched_df["DeviceName"]
        .astype(str)
        .apply(lambda dn: _row_guess(dn, device_prefix_map, variant_to_code))
    )
    dept_guess = [g[0] for g in guesses]
    why_guess = [g[1] for g in guesses]

    out = unmatched_df.copy()
    out["DepartmentGuess"] = pd.Series(dept_guess, index=out.index, dtype="object")
    out["GuessReason"] = pd.Series(why_guess, index=out.index, dtype="object")
    return out


def export_unmatched_grouped(
    unmatched_df_with_guess: pd.DataFrame,
    output_dir: str,
    file_stem: str = "ad_unmatched_grouped",
) -> None:
    """
    Writes a consolidated CSV+JSON and per-department CSVs.
    """
    os.makedirs(output_dir, exist_ok=True)

    consolidated_csv = os.path.join(output_dir, f"{file_stem}.csv")
    consolidated_json = os.path.join(output_dir, f"{file_stem}.json")

    unmatched_df_with_guess.to_csv(consolidated_csv, index=False)
    unmatched_df_with_guess.to_json(consolidated_json, orient="records", indent=2)
    logger.info("Wrote consolidated AD-unmatched classification: %s", consolidated_csv)

    # Per-dept folders
    df = unmatched_df_with_guess.copy()
    df["DepartmentBucket"] = df["DepartmentGuess"].fillna("Unknown")

    per_root = os.path.join(output_dir, "ad_unmatched")
    os.makedirs(per_root, exist_ok=True)

    for dept, gdf in df.groupby("DepartmentBucket"):
        safe = re.sub(r"[^A-Za-z0-9_-]+", "_", str(dept))
        folder = os.path.join(per_root, safe)
        os.makedirs(folder, exist_ok=True)
        gdf.sort_values(["DeviceName"], na_position="last").to_csv(
            os.path.join(folder, "unmatched.csv"), index=False
        )
        logger.info("Wrote AD-unmatched %s devices: %s", dept, folder)


def classify_and_export_unmatched_from_csv(
    unmatched_csv: str,
    output_dir: str,
    device_prefix_map: Dict[str, str],
    variant_to_code: Dict[str, str],
    file_stem: str = "ad_unmatched_grouped",
) -> None:
    """
    Convenience wrapper: read enrichment's unmatched_devices.csv and export grouped outputs.
    """
    if not os.path.isfile(unmatched_csv):
        logger.warning(
            "No unmatched CSV found at %s (skipping grouping).", unmatched_csv
        )
        return
    unmatched_df = pd.read_csv(unmatched_csv)
    if "DeviceName" not in unmatched_df.columns:
        logger.warning("unmatched CSV missing 'DeviceName' column (skipping).")
        return
    out = classify_unmatched_df(unmatched_df, device_prefix_map, variant_to_code)
    export_unmatched_grouped(out, output_dir=output_dir, file_stem=file_stem)
