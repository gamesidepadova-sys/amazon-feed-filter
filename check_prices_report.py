import csv
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------- CONFIG ----------
COUNTRY = (os.environ.get("COUNTRY") or "it").strip().lower()
INPUT_FILTERED = "filtered.csv"  # CSV export locale
GSHEET_ID = os.environ.get("GSHEET_ID")  # ID Google Sheet
SHEET_SETTINGS = "SETTINGS"
SHEET_SELECTION = f"SELEZIONE_{COUNTRY.upper()}"
SHEET_SUPPLIERS = "SUPPLIER_CODES"
OUTPUT_REPORT = f"price_check_report_{COUNTRY}.csv"

# ---------- HELPERS ----------
def money(x: Decimal, decimals: int = 2) -> Decimal:
    q = Decimal("1." + "0" * decimals)
    return x.quantize(q, rounding=ROUND_HALF_UP)

def clean_str(x) -> str:
    return str(x or "").replace("\ufeff", "").replace("\u00a0", " ").strip()

def to_dec(x, default=Decimal("0")):
    try:
        return Decimal(str(x).strip().replace(",", "."))
    except:
        return default

def to_int(x, default=0):
    try:
        return int(str(x).strip())
    except:
        return default

def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name
    ).execute()
    return resp.get("values", [])

def build_index(header: List[str]) -> Dict[str,int]:
    return {clean_str(h).lower(): i for i,h in enumerate(header)}

def get_cell(row, idx, key, default=""):
    i = idx.get(key.lower(), -1)
    if i < 0 or i >= len(row):
        return default
    return clean_str(row[i])

# ---------- LOAD GOOGLE SHEETS ----------
creds = service_account.Credentials.from_service_account_file(
    "sa.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
)
sheets = build("sheets", "v4", credentials=creds)

# SETTINGS
settings_rows = read_sheet(sheets, GSHEET_ID, SHEET_SETTINGS)
settings = {clean_str(r[0]): clean_str(r[1]) for r in settings_rows if len(r) >= 2}
vat_pct = to_dec(settings.get(f"vat_rate_pct_{COUNTRY}", "22"))
b2c_markup_pct = to_dec(settings.get(f"b2c_markup_pct_{COUNTRY}", "28"))
b2b_disc_pct = to_dec(settings.get(f"b2b_discount_vs_b2c_pct_{COUNTRY}", "7"))
qty2_disc_pct = to_dec(settings.get(f"qty2_discount_vs_b2c_pct_{COUNTRY}", "8"))
qty4_disc_pct = to_dec(settings.get(f"qty4_discount_vs_b2c_pct_{COUNTRY}", "9"))
round_decimals = to_int(settings.get(f"price_round_decimals_{COUNTRY}", "2"), 2)

vat_mul = Decimal("1") + vat_pct / Decimal("100")
b2c_mul = Decimal("1") + b2c_markup_pct / Decimal("100")
b2b_mul = Decimal("1") - b2b_disc_pct / Decimal("100")
qty2_mul = Decimal("1") - qty2_disc_pct / Decimal("100")
qty4_mul = Decimal("1") - qty4_disc_pct / Decimal("100")

# SUPPLIERS
supplier_rows = read_sheet(sheets, GSHEET_ID, SHEET_SUPPLIERS)
supplier_idx = build_index(supplier_rows[0])
supplier_ship: Dict[str, Decimal] = {}
for r in supplier_rows[1:]:
    code = get_cell(r, supplier_idx, "supplier_code")
    ship = to_dec(get_cell(r, supplier_idx, "ship_cost_b2c_eur"), Decimal("0"))
    if code:
        supplier_ship[code] = ship

# SELECTION (per verificare quali SKU pubblicare)
sel_rows = read_sheet(sheets, GSHEET_ID, SHEET_SELECTION)
sel_idx = build_index(sel_rows[0])
publish_b2c = set()
publish_b2b = set()
for r in sel_rows[1:]:
    sku = get_cell(r, sel_idx, "sku")
    if sku:
        pub_b2c_flag = get_cell(r, sel_idx, "publish_b2c", "NO").upper() == "YES"
        pub_b2b_flag = get_cell(r, sel_idx, "publish_b2b", "NO").upper() == "YES"
        if pub_b2c_flag: publish_b2c.add(sku)
        if pub_b2b_flag: publish_b2b.add(sku)

# ---------- REPORT CSV ----------
with open(OUTPUT_REPORT, "w", newline="", encoding="utf-8") as fout, \
     open(INPUT_FILTERED, "r", encoding="utf-8-sig") as fin:

    reader = csv.DictReader(fin)
    fieldnames = [
        "SKU", "Base_price", "Shipping", "B2C_calc", "B2C_real", "Diff_B2C",
        "B2B_calc", "B2B_real", "Diff_B2B"
    ]
    writer = csv.DictWriter(fout, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        sku = clean_str(row.get("sku"))
        base = to_dec(row.get("prezzo_iva_esclusa"))
        sup = sku.split("_")[1] if "_" in sku else ""
        ship = supplier_ship.get(sup, Decimal("0"))

        # Calcolo B2C
        b2c_calc = money((base * b2c_mul + ship) * vat_mul, round_decimals)
        b2c_real = to_dec(row.get("price_b2c_eur", "0"))
        diff_b2c = b2c_calc - b2c_real

        # Calcolo B2B
        b2b_calc = money(b2c_calc * b2b_mul, round_decimals)
        qty2_calc = money(b2c_calc * qty2_mul, round_decimals)
        qty4_calc = money(b2c_calc * qty4_mul, round_decimals)
        b2b_real = to_dec(row.get("price_b2b_eur", "0"))
        diff_b2b = b2b_calc - b2b_real

        writer.writerow({
            "SKU": sku,
            "Base_price": base,
            "Shipping": ship,
            "B2C_calc": b2c_calc,
            "B2C_real": b2c_real,
            "Diff_B2C": diff_b2c,
            "B2B_calc": b2b_calc,
            "B2B_real": b2b_real,
            "Diff_B2B": diff_b2b
        })

print(f"✅ Report generato: {OUTPUT_REPORT}")
