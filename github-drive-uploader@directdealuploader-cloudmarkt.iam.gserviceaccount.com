import csv
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_SELECTION = "selezione"
SHEET_SUPPLIERS = "supplier codes"
SHEET_SETTINGS = "settings"

INPUT_FILTERED = "filtered.csv"

OUT_B2C = "amazon_b2c.csv"
OUT_B2B = "amazon_b2b.csv"


def money(x: Decimal, decimals: int = 2) -> Decimal:
    q = Decimal("1." + "0" * decimals)
    return x.quantize(q, rounding=ROUND_HALF_UP)


def norm_yes(x: Any) -> bool:
    return str(x or "").strip().upper() == "YES"


def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name
    ).execute()
    return resp.get("values", [])


def kv_settings(rows: List[List[str]]) -> Dict[str, str]:
    out = {}
    for r in rows:
        if len(r) < 2:
            continue
        k = str(r[0]).strip()
        v = str(r[1]).strip()
        if k:
            out[k] = v
    return out


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


def build_index(header: List[str]) -> Dict[str, int]:
    return {str(h).strip().lower(): i for i, h in enumerate(header)}


def get_cell(row: List[str], idx: Dict[str, int], key: str, default: str = "") -> str:
    i = idx.get(key.lower(), -1)
    if i < 0 or i >= len(row):
        return default
    return str(row[i]).strip()


def main():
    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID is empty or missing")

    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    sheets = build("sheets", "v4", credentials=creds)

    # ===== SETTINGS =====
    settings = kv_settings(read_sheet(sheets, spreadsheet_id, SHEET_SETTINGS))

    vat_pct = to_dec(settings.get("vat_rate_pct", "20"))
    b2c_markup_pct = to_dec(settings.get("b2c_markup_pct", "28"))
    b2b_disc_pct = to_dec(settings.get("b2b_discount_vs_b2c_pct", "7"))
    qty2_disc_pct = to_dec(settings.get("qty2_discount_vs_b2c_pct", "8"))
    qty4_disc_pct = to_dec(settings.get("qty4_discount_vs_b2c_pct", "9"))
    round_decimals = to_int(settings.get("price_round_decimals", "2"), 2)

    vat_mul = Decimal("1") + vat_pct / Decimal("100")
    b2c_mul = Decimal("1") + b2c_markup_pct / Decimal("100")
    b2b_mul = Decimal("1") - b2b_disc_pct / Decimal("100")
    qty2_mul = Decimal("1") - qty2_disc_pct / Decimal("100")
    qty4_mul = Decimal("1") - qty4_disc_pct / Decimal("100")

    # ===== SUPPLIERS =====
    sup_rows = read_sheet(sheets, spreadsheet_id, SHEET_SUPPLIERS)
    sup_idx = build_index(sup_rows[0])

    sup_map = {}
    for r in sup_rows[1:]:
        code = get_cell(r, sup_idx, "supplier_code")
        if code:
            sup_map[code] = {
                "active": get_cell(r, sup_idx, "active", "YES"),
                "lead_b2c_min_days": get_cell(r, sup_idx, "lead_b2c_min_days"),
                "lead_b2c_max_days": get_cell(r, sup_idx, "lead_b2c_max_days"),
                "lead_b2b_min_days": get_cell(r, sup_idx, "lead_b2b_min_days"),
                "lead_b2b_max_days": get_cell(r, sup_idx, "lead_b2b_max_days"),
            }

    # ===== SELEZIONE =====
    sel_rows = read_sheet(sheets, spreadsheet_id, SHEET_SELECTION)
    sel_idx = build_index(sel_rows[0])

    pub_b2c, pub_b2b = set(), set()
    for r in sel_rows[1:]:
        sku = get_cell(r, sel_idx, "sku")
        if norm_yes(get_cell(r, sel_idx, "publish_b2c")):
            pub_b2c.add(sku)
        if norm_yes(get_cell(r, sel_idx, "publish_b2b")):
            pub_b2b.add(sku)

    # ===== FILTERED =====
    with open(INPUT_FILTERED, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)
        delim = "\t" if "\t" in first else ("|" if "|" in first else ",")
        reader = csv.DictReader(fin, delimiter=delim)

        with open(OUT_B2C, "w", encoding="utf-8", newline="") as f1, \
             open(OUT_B2B, "w", encoding="utf-8", newline="") as f2:

            w1 = csv.DictWriter(f1, fieldnames=["sku", "price_b2c_eur", "qty_available"])
            w2 = csv.DictWriter(f2, fieldnames=[
                "sku", "price_b2c_eur", "price_b2b_eur",
                "qty2_price_eur", "qty4_price_eur", "qty_available"
            ])
            w1.writeheader()
            w2.writeheader()

            for row in reader:
                sku = (row.get("sku") or "").strip()
                if not sku:
                    continue

                base = to_dec(row.get("prezzo_iva_esclusa"))
                qty = to_int(row.get("quantita"))

                if sku in pub_b2c:
                    b2c = money(base * b2c_mul * vat_mul, round_decimals)
                    w1.writerow({
                        "sku": sku,
                        "price_b2c_eur": f"{b2c}",
                        "qty_available": qty,
                    })

                if sku in pub_b2b:
                    b2c = money(base * b2c_mul * vat_mul, round_decimals)
                    b2b = money(b2c * b2b_mul, round_decimals)
                    q2 = money(b2c * qty2_mul, round_decimals)
                    q4 = money(b2c * qty4_mul, round_decimals)
                    w2.writerow({
                        "sku": sku,
                        "price_b2c_eur": f"{b2c}",
                        "price_b2b_eur": f"{b2b}",
                        "qty2_price_eur": f"{q2}",
                        "qty4_price_eur": f"{q4}",
                        "qty_available": qty,
                    })

    print("Generated amazon_b2c.csv and amazon_b2b.csv")


if __name__ == "__main__":
    main()
