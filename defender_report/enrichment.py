import json
import os
import socket
import subprocess
import tempfile
import logging
import datetime
import pandas as pd
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

def query_ad_computers(computer_names: List[str]) -> Dict[str, dict]:
    """
    Batch-query AD via PowerShell/ADSI. Returns a map:
    { COMPUTER_NAME: { LastLogonTimestamp, OperatingSystem,
                       IPv4Address, DistinguishedName } }
    """
    # write the list of names to a temporary file
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".txt") as name_file:
        name_file.write("\n".join(computer_names))
        names_path = name_file.name

    # Powershell script that reads names and does an ADSI query
    ps_script = f"""
Add-Type -AssemblyName System.DirectoryServices
$names = Get-Content -Path '{names_path}'
$results = @()
foreach ($cn in $names) {{
    $searcher = New-Object System.DirectoryServices.DirectorySearcher
    $searcher.Filter = "(cn=$cn)"
    $searcher.PropertiesToLoad.AddRange(@('lastLogonTimestamp','operatingSystem','ipv4Address','distinguishedName'))
    $entry = $searcher.FindOne()
    if ($entry) {{
        $p = $entry.Properties
        $obj = [PSCustomObject]@{{
            Name               = $entry.Properties['cn'][0]
            LastLogonTimestamp = $p['lastLogonTimestamp'][0]
            OperatingSystem    = $p['operatingSystem'][0]
            IPv4Address        = $p['ipv4Address'][0]
            DistinguishedName  = $p['distinguishedName'][0]
        }}
        $results += $obj
    }}
}}
$results | ConvertTo-Json -Depth 2
"""
    # write and execute the PS script
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ps1", encoding="utf-8") as ps_file:
        ps_file.write(ps_script)
        ps1_path = ps_file.name

    proc = subprocess.run(
        ["powershell", "-NoProfile", "-File", ps1_path],
        capture_output=True,
        text=True
    )

    # clean up
    os.unlink(names_path)
    os.unlink(ps1_path)

    if proc.returncode != 0:
        logger.error("AD batch failed: %s", proc.stderr.strip())
        return {}

    try:
        entries = json.loads(proc.stdout) if proc.stdout else []
    except json.JSONDecodeError:
        logger.error("Invalid JSON from AD query: %.200s", proc.stdout)
        return {}

    ad_map: Dict[str, dict] = {}
    for entry in (entries if isinstance(entries, list) else [entries]):
        name = entry.get("Name")
        if name:
            ad_map[name] = entry
    return ad_map

def convert_ad_timestamp(filetime: Optional[int]) -> Optional[datetime.datetime]:
    """
    Convert Windows FILETIME (100-ns intervals since 1601-01-01) to datetime.
    """
    if not filetime:
        return None
    microseconds = int(filetime) // 10
    return datetime.datetime(1601, 1, 1) + datetime.timedelta(microseconds=microseconds)

def parse_ou(distinguished_name: Optional[str]) -> str:
    """
    Extract the first OU component from a DistinguishedName string.
    """
    if not distinguished_name:
        return "Unknown"
    for part in distinguished_name.split(","):
        if part.strip().upper().startswith("OU="):
            return part.strip()[3:]
    return "Unknown"

def get_ipv4_address(hostname: str) -> str:
    """
    Fallback DNS lookup for machines that did not report an IPv4Address in AD.
    """
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return "N/A"

def enrich_all_sheets_with_ad(
    all_sheets: Dict[str, pd.DataFrame]
) -> Dict[str, pd.DataFrame]:
    """
    For each sheet in `all_sheets`, append AD columns by querying once
    for every unique DeviceName.
    """
    # collect all unique names
    unique_names = {
        str(name).strip()
        for df in all_sheets.values()
        for name in df["DeviceName"].dropna().unique()
        if str(name).strip()
    }
    ad_info_map = query_ad_computers(sorted(unique_names))

    enriched_sheets: Dict[str, pd.DataFrame] = {}
    for sheet_name, df in all_sheets.items():
        df_copy = df.copy()
        last_logon_list, os_list, ip_list, ou_list = [], [], [], []

        for _, row in df_copy.iterrows():
            machine_name = str(row.get("DeviceName", "")).strip()
            info = ad_info_map.get(machine_name, {})
            last_logon_list.append(convert_ad_timestamp(info.get("LastLogonTimestamp")))
            os_list.append(info.get("OperatingSystem"))
            ip_list.append(info.get("IPv4Address") or get_ipv4_address(machine_name))
            ou_list.append(parse_ou(info.get("DistinguishedName")))

        df_copy["LastLogonDate"] = last_logon_list
        df_copy["OperatingSystem"] = os_list
        df_copy["OUName"] = ou_list
        df_copy["IPv4Address"] = ip_list
        enriched_sheets[sheet_name] = df_copy

    return enriched_sheets
