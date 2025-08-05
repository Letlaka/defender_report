import os
import sys
import json
import logging
import datetime
import pathlib
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm

import pandas as pd
from dotenv import load_dotenv
from ldap3 import Server, Connection, ALL, SUBTREE

logger = logging.getLogger(__name__)


def get_project_root() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return str(pathlib.Path(__file__).resolve().parents[1])


# Load .env
load_dotenv(os.path.join(get_project_root(), ".env"))

AD_SERVER = os.getenv("AD_SERVER")
AD_USERNAME = os.getenv("AD_USERNAME")
AD_PASSWORD = os.getenv("AD_PASSWORD")
AD_BASE_DN = os.getenv("AD_BASE_DN")

CACHE_FILE = os.path.join(get_project_root(), ".ad_cache.json")


def get_batch_size(device_count: int) -> int:
    if device_count > 10000:
        return 100
    elif device_count > 5000:
        return 150
    elif device_count > 1000:
        return 200
    return 300


def get_first(attr: dict, key: str) -> Optional[str]:
    val = attr.get(key)
    return val[0] if isinstance(val, list) and val else None


def convert_ad_timestamp(filetime: Optional[str]) -> Optional[str]:
    if not filetime:
        return None
    try:
        microseconds = int(filetime) // 10
        dt = datetime.datetime(1601, 1, 1) + datetime.timedelta(microseconds=microseconds)
        return dt.isoformat()
    except Exception as e:
        logger.warning(f"Failed to convert AD timestamp '{filetime}': {e}")
        return None


def parse_ou_path(dn: Optional[str]) -> str:
    if not dn:
        return "Unknown"
    return "/".join([part[3:] for part in dn.split(",") if part.strip().upper().startswith("OU=")])

def query_ad_computers(computer_names: List[str]) -> Tuple[Dict[str, dict], List[str]]:
    if not all([AD_SERVER, AD_USERNAME, AD_PASSWORD, AD_BASE_DN]):
        logger.error("Missing LDAP configuration in environment.")
        return {}, list(computer_names)

    # Load cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}

    ad_map: Dict[str, dict] = {}
    unmatched: List[str] = []
    to_query = sorted({name for name in computer_names if name and name not in cache})

    try:
        server = Server(str(AD_SERVER), get_info=ALL)
        conn = Connection(server, user=AD_USERNAME, password=AD_PASSWORD,
                          authentication="SIMPLE", auto_bind=True)
    except Exception as e:
        logger.error(f"LDAP bind failed: {e}")
        return {}, to_query

    batch_size = get_batch_size(len(to_query))
    logger.info(f"Querying {len(to_query)} new devices in batches of {batch_size}...")

    def chunked(items: List[str], size: int) -> List[List[str]]:
        return [items[i:i + size] for i in range(0, len(items), size)]

    for batch in tqdm(chunked(to_query, batch_size), desc="AD Lookup", unit="batch"):
        search_filter = f"(|{''.join(f'(cn={name})' for name in batch)})"
        attributes = ["cn", "lastLogonTimestamp", "operatingSystem", "distinguishedName"]

        try:
            search_base = AD_BASE_DN or ""
            if conn.search(search_base, search_filter, search_scope=SUBTREE, attributes=attributes):
                found = {get_first(e.entry_attributes_as_dict, "cn"): e.entry_attributes_as_dict for e in conn.entries}

                for name in batch:
                    entry = found.get(name)
                    if not entry:
                        unmatched.append(name)
                        continue
                    raw_ts = get_first(entry, "lastLogonTimestamp")
                    if isinstance(raw_ts, datetime.datetime):
                        timestamp = raw_ts.isoformat()
                    elif raw_ts:
                        try:
                            timestamp = convert_ad_timestamp(str(raw_ts))
                        except Exception as e:
                            logger.warning(f"Failed to convert lastLogonTimestamp for {name}: {raw_ts} → {e}")
                            timestamp = None
                    else:
                        timestamp = None

                    logger.debug(f"{name}: raw_ts={raw_ts} → parsed={timestamp}")                    
                    cache[name] = {
                        "Name": name.strip(),
                        "LastLogonTimestamp": timestamp,
                        "OperatingSystem": get_first(entry, "operatingSystem"),
                        "DistinguishedName": get_first(entry, "distinguishedName"),
                    }
            else:
                unmatched.extend(batch)
        except Exception as e:
            logger.warning(f"Batch failed: {e}")
            unmatched.extend(batch)

    conn.unbind()

    # Save updated cache
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

    ad_map.update(cache)
    logger.info("LDAP query complete. Total enriched devices: %d", len(ad_map))
    return ad_map, unmatched


def enrich_all_sheets_with_ad(
    all_sheets: Dict[str, pd.DataFrame],
    export_dir: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    all_names = {
        str(name).strip()
        for df in all_sheets.values()
        if not df.empty and "DeviceName" in df.columns
        for name in df["DeviceName"].dropna().unique()
    }
    if not all_names:
        logger.warning("No device names found for AD enrichment.")
        return all_sheets

    ad_map, unmatched = query_ad_computers(sorted(all_names))

    enriched_sheets: Dict[str, pd.DataFrame] = {}
    for sheet_name, df in all_sheets.items():
        if df.empty or "DeviceName" not in df.columns:
            enriched_sheets[sheet_name] = df.copy()
            continue

        df_copy = df.copy()
        def safe_get(field: str, name: str) -> Optional[str]:
            record = ad_map.get(name)
            if isinstance(record, dict):
                return record.get(field)
            return None        
        df_copy["LastLogonDate"] = df_copy["DeviceName"].map(lambda n: safe_get("LastLogonTimestamp", str(n)))
        df_copy["OperatingSystem"] = df_copy["DeviceName"].map(lambda n: safe_get("OperatingSystem", str(n)))
        df_copy["OUName"] = df_copy["DeviceName"].map(lambda n: parse_ou_path(ad_map.get(n, {}).get("DistinguishedName")))

        enriched_sheets[sheet_name] = df_copy

    # Export unmatched
    if unmatched:
        unmatched_df = pd.DataFrame({"DeviceName": unmatched})
        csv_path = os.path.join(export_dir or os.getcwd(), "unmatched_devices.csv")
        json_path = os.path.join(export_dir or os.getcwd(), "unmatched_devices.json")
        unmatched_df.to_csv(csv_path, index=False)
        unmatched_df.to_json(json_path, orient="records", indent=2)
        logger.warning(f"{len(unmatched)} unmatched devices written to:")
        logger.warning(f"  - {csv_path}")
        logger.warning(f"  - {json_path}")

    return enriched_sheets
