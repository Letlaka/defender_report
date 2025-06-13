# Defender Report

A command-line tool to generate per-department Microsoft Defender Agents reports with signature freshness and compliance summaries, optional Active Directory enrichment, and neatly organized output folders by financial year, quarter, month, and date.

---

## Features

- **Master Report**: Single Excel file (`DefenderAgents_Report.xlsx`) with one sheet per department plus a **Summary** sheet.
- **Department Reports**: Individual Excel files for each department, saved under:

    ```text
    <department>/
      <financial_year>/    # e.g. 2024-2025 (Apr–Mar)
        Q<1–4>/            # Apr–Jun=Q1, Jul–Sep=Q2, Oct–Dec=Q3, Jan–Mar=Q4
          <MonthName>/     # e.g. June
            <YYYY-MM-DD>/  # e.g. 2025-06-13
              <dept>_Report_<YYYY-MM-DD>.xlsx
    ```

- **Status & Tally**:  
  Classifies each device as **UpToDate** or **OutOfDate** based on signature freshness, then computes:
  - DeviceCount  
  - Co-managed, Intune, SCCM managed, Inactive clients  
  - Up to Date / Out of Date counts  
  - Compliance rate
- **Active Directory Enrichment** (optional):  
  Appends `LastLogonDate`, `OperatingSystem`, `IPv4Address` and `OUName` from AD via PowerShell.
- **Beautiful Summary Tables**:  
  - Uses **Table Style Medium 16 (blue)** with filter dropdowns and a built-in total row  
  - Applies **≥ 80%** green, **70–80%** yellow, **< 70%** red fills (`#00b050`, `#ffff00`, `#ff0000`)  
  - Includes a “Baseline 80%” legend  
  - Shows the report date (e.g. “13-Jun”) in a colored header

---

## Requirements

- **Python 3.13** or newer  
- Dependencies (installed via `uv sync` or `pip install -r requirements.txt`):
  - `pandas>=2.3.0`  
  - `openpyxl>=3.1.5`  
  - `xlsxwriter>=3.2.3`  
  - `tqdm>=4.67.1`  

---

## Installation

### 1. Clone the repo

```bash
git clone https://github.com/your-org/defender_report.git
cd defender_report
