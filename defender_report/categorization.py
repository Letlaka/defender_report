import datetime
from typing import Dict

import pandas as pd

def categorize_dataframe(
    data_frame: pd.DataFrame,
    reference_date: datetime.date,
    threshold_days: int,
) -> pd.DataFrame:
    """
    Add compliance columns based on reporting time.
    Adds:
        - Status ("UpToDate"/"OutOfDate")
        - ComplianceLevel
        - ComplianceSeverity
        - ComplianceReason
    """
    cutoff_date = reference_date - datetime.timedelta(days=threshold_days)
    df = data_frame.copy()
    df["LastReportedDateTime"] = pd.to_datetime(df.get("LastReportedDateTime", ""), errors="coerce")

    def assess_row(row):
        last_reported = row.get("LastReportedDateTime")
        if pd.isna(last_reported):
            return {
                "Status": "OutOfDate",
                "ComplianceLevel": "Critical",
                "ComplianceSeverity": "critical",
                "ComplianceReason": "No check-in date",
            }
        if last_reported.date() >= cutoff_date:
            return {
                "Status": "UpToDate",
                "ComplianceLevel": "Fully Compliant",
                "ComplianceSeverity": "ok",
                "ComplianceReason": "Device is up to date",
            }
        else:
            return {
                "Status": "OutOfDate",
                "ComplianceLevel": "Not Compliant",
                "ComplianceSeverity": "critical",
                "ComplianceReason": "Missed check-in window",
            }

    assess_results = df.apply(assess_row, axis=1, result_type="expand")
    for col in ["Status", "ComplianceLevel", "ComplianceSeverity", "ComplianceReason"]:
        df[col] = assess_results[col]
    return df

def tally_dataframe(data_frame: pd.DataFrame) -> Dict[str, float]:
    """
    Count devices by management type and freshness, returning counts
    plus a "Compliance" fraction between 0.0 and 1.0.
    """
    valid_mask = data_frame["DeviceName"].astype(str).str.strip() != ""
    device_count = int(valid_mask.sum())

    managed_by = data_frame.get("_ManagedBy", pd.Series()).fillna("").str.lower().str.strip()
    co_managed = int(managed_by.str.contains("co-managed|comanaged").sum())
    intune_only = int(managed_by.str.contains("intune").sum())
    sccm_managed = device_count - co_managed - intune_only

    up_to_date = int((data_frame["Status"] == "UpToDate")[valid_mask].sum())
    out_of_date = int((data_frame["Status"] == "OutOfDate")[valid_mask].sum())
    compliance_rate = (up_to_date / device_count) if device_count else 0.0

    return {
        "DeviceCount": device_count,
        "Co-managed": co_managed,
        "Intune": intune_only,
        "SCCM Managed": sccm_managed,
        "Up to Date": up_to_date,
        "Out of Date": out_of_date,
        "Compliance": compliance_rate,
    }
