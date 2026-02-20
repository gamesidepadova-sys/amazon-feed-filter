import csv
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---- CONFIG ----
COUNTRY = (os.environ.get("COUNTRY") or "it").strip().lower()
SHEET_SELECTION = os.environ.get("SHEET_SELECTION_NAME", f"SELEZIONE_{COUNTRY.upper()}")
SHEET_SETTINGS = os.environ.get("SHEET_SETTINGS_NAME", "SETTINGS")
SHEET_SUPPLIERS = "SUPPLIER_CODES"
INPUT_FILTERED = "filtered.csv"

FAIL_IF_NO_MATCH = (os.environ.get("FAIL_IF_NO_MATCH", "0").strip() == "1")


# ----------------------------
# Helpers base
# ----------------------------
def money(x: Decimal, decimals: int = 2) -> Decimal:
    q = Decimal("1." + "0" * decimals)
    return x.quantize(q, rounding=ROUND_HALF_UP)


def norm_yes(x: Any) -> bool:
    return str(x or "").strip().upper() == "YES"


def to_int(x: Any, default: int = 0) -> int:
    try:
        return int(str(x).strip())
    except Exception:
        return default


def to_dec(x: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        s = str(x).strip().replace(",", ".")
        if not s:
            return default
        return Decimal(s)
    except Exception:
        return default


def clean_str(x: Any) -> str:
    s = str(x or "")
    s = s.replace("\ufeff", "")
    s = s.replace("\u00a0", " ")
    return s.strip()


# ----------------------------
# Google Sheets helpers
# ----------------------------
def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name
    ).execute()
    return resp.get("values", [])


def kv_settings(rows: List[List[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for r in rows:
        if len(r) < 2:
            continue
        k = clean_str(r[0])
        v = clean_str(r[1])
        if k:
            out[k] = v
    return out


def build_index(header: List[str]) -> Dict[str, int]:
    return {clean_str(h).lower(): i for i, h in enumerate(header)}


def get_cell(row: List[str], idx: Dict[str, int], key: str, default: str = "") -> str:
    i = idx.get(key.lower(), -1)
    if i < 0 or i >= len(row):
        return default
    return clean_str(row[i])


def get_setting(settings: Dict[str, str], key: str, country: str, default: str) -> str:
    key_country = f"{key}_{country}".lower()
    for k, v in settings.items():
        if k.lower() == key_country:
            return v
    for k, v in settings.items():
        if k.lower() == key.lower():
            return v
    return default


def find_sheet_tab_case_insensitive(sheets_service, spreadsheet_id: str, desired: str) -> str:
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    tabs = [s.get("properties", {}).get("title", "") for s in (meta.get("sheets") or [])]
    desired_l = desired.strip().lower()
    for t in tabs:
        if t.strip().lower() == desired_l:
            return t
    print(f"WARNING: tab '{desired}' not found exactly. Available tabs: {tabs}")
    return desired


# ----------------------------
# NEW: Load suppliers from Google Sheet
# ----------------------------
def load_supplier_sheet(service, spreadsheet_id: str, sheet_name: str):
    rows = read_sheet(service, spreadsheet_id, sheet_name)
    if not rows:
        print(f"WARNING: supplier sheet '{sheet_name}' empty")
        return {}, {}

    header = rows[0]
    idx = build_index(header)

    out_handling = {}
    out_ship = {}

    for r in rows[1:]:
        code = clean_str(get_cell(r, idx, "supplier_code"))
        if not code:
            continue

        active = clean_str(get_cell(r, idx, "active", "1")).lower()
        if active in {"0", "no", "false"}:
            continue

        handling = to_int(get_cell(r, idx, "lead_b2c_max_days"), 2)
        ship = to_dec(get_cell(r, idx, "ship_cost_b2c_eur"), Decimal("0"))

        out_handling[code] = handling
        out_ship[code] = ship

    return out_handling, out_ship


# ----------------------------
# CSV helpers
# ----------------------------
def detect_delim_from_first_line(first_line: str) -> str:
    if "\t" in first_line:
        return "\t"
    if "|" in first_line:
        return "|"
    if ";" in first_line:
        return ";"
    return ","


def normalize_fieldnames(fieldnames: List[str]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    for f in fieldnames or []:
        norm = clean_str(f).lower().replace(" ", "_")
        m[norm] = f
    return m


def pick_field(field_map: Dict[str, str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        c0 = c.lower().replace(" ", "_")
        if c0 in field_map:
