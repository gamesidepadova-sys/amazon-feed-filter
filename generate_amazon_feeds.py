import csv
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_SELECTION = "SELEZIONE"
SHEET_SETTINGS = "settings"
INPUT_FILTERED = "filtered.csv"
SUPPLIERS_FILE = "suppliers.csv"


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
    out = {}
    for r in rows:
        if len(r) < 2:
            continue
        k = str(r[0]).strip()
        v = str(r[1]).strip()
        if k:
            out[k] = v
    return out


def build_index(header: List[str]) -> Dict[str, int]:
    return {str(h).strip().lower(): i for i, h in enumerate(header)}


def get_cell(row: List[str], idx: Dict[str, int], key: str, default: str = "") -> str:
    i = idx.get(key.lower(), -1)
    if i < 0 or i >= len(row):
        return default
    return str(row[i]).strip()


def get_setting(settings: Dict[str, str], key: str, country: str, default: str) -> str:
    key_country = f"{key}_{country}".lower()
    for k, v in settings.items():
        if k.lower() == key_country:
            return v
    for k, v in settings.items():
        if k.lower() == key.lower():
            return v
    return default


# ----------------------------
# MAIN
# ----------------------------
def main():
    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID missing")

    seller_id = os.environ.get("AMAZON_SELLER_ID", "").strip()
    if not seller_id:
        raise RuntimeError("AMAZON_SELLER_ID missing")

    country = os.environ.get("COUNTRY", "it").strip().lower()

    out_b2c = f"amazon_{country}_b2c.csv"
    out_b2b = f"amazon_{country}_b2b.csv"
    out_priceinv = f"amazon_{country}_price_quantity.txt"
    out_listings = f"amazon_{country}_listings.json"

    # Google Sheets
    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    sheets = build("sheets", "v4", credentials=creds)

    settings = kv_settings(read_sheet(sheets, spreadsheet_id, SHEET_SETTINGS))

    vat_pct = to_dec(get_setting(settings, "vat_rate_pct", country, "22"))
    b2c_markup_pct = to_dec(get_setting(settings, "b2c_markup_pct", country, "28"))
    b2b_disc_pct = to_dec(get_setting(settings, "b2b_discount_vs_b2c_pct", country, "7"))
    round_decimals = to_int(get_setting(settings, "price_round_decimals", country, "2"), 2)

    vat_mul = Decimal("1") + vat_pct / Decimal("100")
    b2c_mul = Decimal("1") + b2c_markup_pct / Decimal("100")
    b2b_mul = Decimal("1") - b2b_disc_pct / Decimal("100")

    # ---- SELEZIONE ----
    sel_rows = read_sheet(sheets, spreadsheet_id, SHEET_SELECTION)
    if not sel_rows:
        raise RuntimeError(f'Sheet "{SHEET_SELECTION}" empty')

    sel_idx = build_index(sel_rows[0])

    pub_b2c, pub_b2b = set(), set()
    for r in sel_rows[1:]:
        sku = get_cell(r, sel_idx, "sku")
        if not sku:
            continue
        if norm_yes(get_cell(r, sel_idx, "publish_b2c")):
            pub_b2c.add(sku)
        if norm_yes(get_cell(r, sel_idx, "publish_b2b")):
            pub_b2b.add(sku)

    print("DEBUG selection sizes:", len(pub_b2c), len(pub_b2b))

    # ---- filtered.csv ----
    with open(INPUT_FILTERED, "r", encoding="utf-8-sig", newline="") as fin:
        reader = csv.DictReader(fin)

        rows_b2c = 0
        rows_b2b = 0
        listings_messages = []
        msg_id = 1

        with open(out_b2c, "w", encoding="utf-8", newline="") as f1, \
             open(out_b2b, "w", encoding="utf-8", newline="") as f2:

            w1 = csv.DictWriter(f1, ["sku", "price_b2c_eur", "qty_available", "country"])
            w2 = csv.DictWriter(f2, ["sku", "price_b2c_eur", "price_b2b_eur", "qty_available", "country"])

            w1.writeheader()
            w2.writeheader()

            for row in reader:
                sku = (row.get("sku") or "").strip()
                if not sku:
                    continue
                if sku not in pub_b2c and sku not in pub_b2b:
                    continue

                base = to_dec(row.get("prezzo_iva_esclusa"))
                qty = to_int(row.get("quantita"))

                if base <= 0:
                    continue

                b2c = money((base * b2c_mul) * vat_mul, round_decimals)

                if sku in pub_b2c:
                    w1.writerow({
                        "sku": sku,
                        "price_b2c_eur": f"{b2c}",
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2c += 1

                if sku in pub_b2b:
                    b2b = money(b2c * b2b_mul, round_decimals)
                    w2.writerow({
                        "sku": sku,
                        "price_b2c_eur": f"{b2c}",
                        "price_b2b_eur": f"{b2b}",
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2b += 1

                # JSON_LISTINGS_FEED
                listings_messages.append({
                    "messageId": msg_id,
                    "sku": sku,
                    "operationType": "PATCH",
                    "productType": "PRODUCT",
                    "patches": [
                        {
                            "op": "replace",
                            "path": "/attributes/fulfillment_availability",
                            "value": [{
                                "fulfillment_channel_code": "DEFAULT",
                                "quantity": qty
                            }]
                        },
                        {
                            "op": "replace",
                            "path": "/attributes/purchasable_offer",
                            "value": [{
                                "currency": "EUR",
                                "our_price": [{
                                    "schedule": [{
                                        "value_with_tax": float(b2c)
                                    }]
                                }]
                            }]
                        }
                    ]
                })
                msg_id += 1

    # --- write JSON ---
    listings_obj = {
        "header": {
            "sellerId": seller_id,
            "version": "2.0",
            "issueLocale": "it_IT"
        },
        "messages": listings_messages
    }

    with open(out_listings, "w", encoding="utf-8") as fjson:
        json.dump(listings_obj, fjson, ensure_ascii=False, indent=2)

    print(f"[{country}] Generated {out_b2c} rows={rows_b2c}")
    print(f"[{country}] Generated {out_b2b} rows={rows_b2b}")
    print(f"[{country}] Generated {out_listings} messages={len(listings_messages)}")


if __name__ == "__main__":
    main()
