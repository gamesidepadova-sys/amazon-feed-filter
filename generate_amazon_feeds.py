import csv
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build


# =============================
# CONFIG
# =============================
SHEET_SELECTION = "selezione"
SHEET_SETTINGS = "settings"
INPUT_FILTERED = "filtered.csv"
SUPPLIERS_FILE = "suppliers.csv"


# =============================
# Helpers
# =============================

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


def supplier_code_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    return parts[1] if len(parts) >= 2 else ""


# =============================
# Google Sheets helpers
# =============================

def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name
    ).execute()
    return resp.get("values", [])


def build_index(header: List[str]) -> Dict[str, int]:
    return {str(h).strip().lower(): i for i, h in enumerate(header)}


def get_cell(row: List[str], idx: Dict[str, int], key: str, default: str = "") -> str:
    i = idx.get(key.lower(), -1)
    if i < 0 or i >= len(row):
        return default
    return str(row[i]).strip()


# =============================
# Suppliers
# =============================

def load_supplier_data(path: str):
    handling = {}
    ship_cost = {}

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                code = (row.get("supplier_code") or "").strip()
                if not code:
                    continue
                try:
                    handling[code] = int(row.get("lead_b2c_max_days", 2))
                except:
                    handling[code] = 2

                try:
                    ship_cost[code] = Decimal(str(row.get("ship_cost_b2c_eur")).replace(",", "."))
                except:
                    ship_cost[code] = Decimal("0")

    except FileNotFoundError:
        pass

    return handling, ship_cost


# =============================
# MAIN
# =============================

def main():

    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    country = os.environ.get("COUNTRY", "it").strip().lower()
    seller_id = os.environ.get("AMAZON_SELLER_ID", "").strip()

    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID missing")

    if not seller_id:
        raise RuntimeError("AMAZON_SELLER_ID missing")

    # =============================
    # Google auth
    # =============================
    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    sheets = build("sheets", "v4", credentials=creds)

    # =============================
    # Pricing settings (hardcoded base)
    # =============================
    vat_pct = Decimal("22")
    b2c_markup_pct = Decimal("28")
    b2b_discount_pct = Decimal("7")
    round_decimals = 2

    vat_mul = Decimal("1") + vat_pct / 100
    b2c_mul = Decimal("1") + b2c_markup_pct / 100
    b2b_mul = Decimal("1") - b2b_discount_pct / 100

    # =============================
    # Selezione sheet
    # =============================
    sel_rows = read_sheet(sheets, spreadsheet_id, SHEET_SELECTION)
    if not sel_rows:
        raise RuntimeError("Selezione sheet empty")

    sel_idx = build_index(sel_rows[0])

    pub_b2c = set()
    pub_b2b = set()

    for r in sel_rows[1:]:
        sku = get_cell(r, sel_idx, "sku")
        if not sku:
            continue

        if norm_yes(get_cell(r, sel_idx, "publish_b2c")):
            pub_b2c.add(sku)

        if norm_yes(get_cell(r, sel_idx, "publish_b2b")):
            pub_b2b.add(sku)

    # =============================
    # Suppliers
    # =============================
    supplier_handling, supplier_ship = load_supplier_data(SUPPLIERS_FILE)

    # =============================
    # JSON Listings structure
    # =============================
    listings = {
        "header": {
            "sellerId": seller_id,
            "version": "2.0",
            "issueLocale": "it_IT"
        },
        "messages": []
    }

    message_id = 1

    # =============================
    # Read filtered.csv
    # =============================
    with open(INPUT_FILTERED, "r", encoding="utf-8-sig") as fin:

        reader = csv.DictReader(fin)

        for row in reader:

            sku = (row.get("sku") or "").strip()
            if not sku:
                continue

            if sku not in pub_b2c and sku not in pub_b2b:
                continue

            base = to_dec(row.get("prezzo_iva_esclusa"))
            qty = to_int(row.get("quantita"))

            if base <= 0 or qty < 0:
                continue

            supplier = supplier_code_from_sku(sku)
            ship_cost = supplier_ship.get(supplier, Decimal("0"))
            handling_days = supplier_handling.get(supplier, 2)

            # B2C price
            b2c_price = money((base * b2c_mul + ship_cost) * vat_mul, round_decimals)

            # =============================
            # JSON PATCH
            # =============================
            message = {
                "messageId": message_id,
                "sku": sku,
                "operationType": "PATCH",
                "productType": "PRODUCT",
                "patches": [
                    {
                        "op": "replace",
                        "path": "/attributes/fulfillment_availability",
                        "value": [
                            {
                                "fulfillment_channel_code": "DEFAULT",
                                "quantity": qty,
                                "handling_time": handling_days
                            }
                        ]
                    },
                    {
                        "op": "replace",
                        "path": "/attributes/purchasable_offer",
                        "value": [
                            {
                                "currency": "EUR",
                                "our_price": [
                                    {
                                        "schedule": [
                                            {
                                                "value_with_tax": float(b2c_price)
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }

            listings["messages"].append(message)
            message_id += 1

    # =============================
    # Save JSON
    # =============================
    output_json = f"amazon_{country}_listings.json"

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(listings, f, indent=2)

    print(f"[{country}] Generated {output_json} messages={len(listings['messages'])}")


if __name__ == "__main__":
    main()
