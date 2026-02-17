import csv
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_SELECTION = "SELEZIONE"      # <-- il tuo tab si chiama così (maiuscolo)
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
# Suppliers helpers
# ----------------------------
def _norm_bool(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y"}


def supplier_code_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    return parts[1] if len(parts) >= 2 else ""


def load_supplier_handling_max_days(path: str) -> Dict[str, int]:
    out = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                if not _norm_bool(row.get("active")):
                    continue
                code = (row.get("supplier_code") or "").strip()
                if not code:
                    continue
                try:
                    out[code] = int(str(row.get("lead_b2c_max_days", "2")).strip())
                except Exception:
                    out[code] = 2
    except FileNotFoundError:
        pass
    return out


def load_supplier_ship_cost_b2c(path: str) -> Dict[str, Decimal]:
    out = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                code = (row.get("supplier_code") or "").strip()
                if not code:
                    continue
                try:
                    out[code] = Decimal(str(row.get("ship_cost_b2c_eur", "0")).replace(",", "."))
                except Exception:
                    out[code] = Decimal("0")
    except FileNotFoundError:
        pass
    return out


# ----------------------------
# MAIN
# ----------------------------
def main():
    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID missing")

    seller_id = os.environ.get("AMAZON_SELLER_ID", "").strip()
    if not seller_id:
        raise RuntimeError("AMAZON_SELLER_ID missing (sellerId obbligatorio per JSON_LISTINGS_FEED)")

    country = os.environ.get("COUNTRY", "it").strip().lower()
    if country not in {"it", "de", "fr", "es"}:
        raise RuntimeError("COUNTRY must be one of: it,de,fr,es")

    out_b2c = f"amazon_{country}_b2c.csv"
    out_b2b = f"amazon_{country}_b2b.csv"
    out_priceinv = f"amazon_{country}_price_quantity.txt"      # legacy/debug
    out_listings = f"amazon_{country}_listings.json"           # JSON_LISTINGS_FEED

    # suppliers maps
    supplier_handling = load_supplier_handling_max_days(SUPPLIERS_FILE)
    supplier_ship_cost = load_supplier_ship_cost_b2c(SUPPLIERS_FILE)

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
    qty2_disc_pct = to_dec(get_setting(settings, "qty2_discount_vs_b2c_pct", country, "8"))
    qty4_disc_pct = to_dec(get_setting(settings, "qty4_discount_vs_b2c_pct", country, "9"))
    round_decimals = to_int(get_setting(settings, "price_round_decimals", country, "2"), 2)

    vat_mul = Decimal("1") + vat_pct / Decimal("100")
    b2c_mul = Decimal("1") + b2c_markup_pct / Decimal("100")
    b2b_mul = Decimal("1") - b2b_disc_pct / Decimal("100")
    qty2_mul = Decimal("1") - qty2_disc_pct / Decimal("100")
    qty4_mul = Decimal("1") - qty4_disc_pct / Decimal("100")

    # ---- SELEZIONE ----
    sel_rows = read_sheet(sheets, spreadsheet_id, SHEET_SELECTION)
    if not sel_rows:
        raise RuntimeError(f'Sheet "{SHEET_SELECTION}" empty or missing in GSHEET')

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

    # ---- filtered.csv ----
    with open(INPUT_FILTERED, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)

        if "\t" in first:
            delim = "\t"
        elif "|" in first:
            delim = "|"
        elif ";" in first:
            delim = ";"
        else:
            delim = ","

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError("filtered.csv has no header")

        # --- DEBUG counters ---
        skipped_missing_sku = 0
        skipped_not_selected = 0
        skipped_missing_fields = 0
        skipped_bad_base_qty = 0
        matched = 0

        print("DEBUG filtered.csv fieldnames:", reader.fieldnames)
        print("DEBUG selection sizes:", "pub_b2c=", len(pub_b2c), "pub_b2b=", len(pub_b2b))

        rows_b2c = 0
        rows_b2b = 0
        rows_priceinv = 0

        listings_messages = []
        msg_id = 1

        with open(out_b2c, "w", encoding="utf-8", newline="") as f1, \
             open(out_b2b, "w", encoding="utf-8", newline="") as f2, \
             open(out_priceinv, "w", encoding="utf-8", newline="") as f3:

            w1 = csv.DictWriter(f1, ["sku", "price_b2c_eur", "qty_available", "country"])
            w2 = csv.DictWriter(f2, [
                "sku", "price_b2c_eur", "price_b2b_eur",
                "qty2_price_eur", "qty4_price_eur", "qty_available", "country"
            ])
            w3 = csv.writer(f3, delimiter="\t", lineterminator="\n")

            w1.writeheader()
            w2.writeheader()
            w3.writerow([
                "sku",
                "price",
                "minimum-seller-allowed-price",
                "maximum-seller-allowed-price",
                "quantity",
                "fulfillment-channel",
                "handling-time",
            ])

            for row in reader:
                sku = (row.get("sku") or "").strip()

                if not sku:
                    skipped_missing_sku += 1
                    continue

                if sku not in pub_b2c and sku not in pub_b2b:
                    skipped_not_selected += 1
                    continue

                raw_base = row.get("prezzo_iva_esclusa")
                raw_qty = row.get("quantita")

                if raw_base is None or raw_qty is None:
                    skipped_missing_fields += 1
                    continue

                base = to_dec(raw_base)
                qty = to_int(raw_qty)

                if base <= 0 or qty < 0:
                    skipped_bad_base_qty += 1
                    continue

                matched += 1

                sup = supplier_code_from_sku(sku)
                ship = supplier_ship_cost.get(sup, Decimal("0"))
                handling = supplier_handling.get(sup, 2)

                # Prezzo finale: (base * markup + ship) * IVA
                b2c = money((base * b2c_mul + ship) * vat_mul, round_decimals)

                # --- B2C CSV ---
                if sku in pub_b2c:
                    w1.writerow({
                        "sku": sku,
                        "price_b2c_eur": f"{b2c}",
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2c += 1

                # --- B2B CSV ---
                if sku in pub_b2b:
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
                        "country": country
                    })
                    rows_b2b += 1

                # --- legacy tab feed (debug) ---
                # NB: non più valido come feedType, ma lo teniamo per backup/controllo.
                w3.writerow([sku, f"{b2c}", "", "", str(qty), "DEFAULT", str(handling)])
                rows_priceinv += 1

                # --- JSON_LISTINGS_FEED message ---
                # Nota: qui aggiorniamo price + quantity MFN.
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


    # --- write JSON listings feed ---
    listings_obj = {
        "header": {
            "sellerId": seller_id,
            "version": "2.0",
            "issueLocale": "it_IT" if country == "it" else "en_GB"
        },
        "messages": listings_messages
    }

    with open(out_listings, "w", encoding="utf-8") as fjson:
        json.dump(listings_obj, fjson, ensure_ascii=False, indent=2)

    print(f"[{country}] Generated {out_b2c} rows={rows_b2c}")
    print(f"[{country}] Generated {out_b2b} rows={rows_b2b}")
    print(f"[{country}] Generated {out_priceinv} (legacy/debug) rows={rows_priceinv}")
    print(f"[{country}] Generated {out_listings} messages={len(listings_messages)}")

    print("DEBUG matched:", matched)
    print("DEBUG skipped_missing_sku:", skipped_missing_sku)
    print("DEBUG skipped_not_selected:", skipped_not_selected)
    print("DEBUG skipped_missing_fields:", skipped_missing_fields)
    print("DEBUG skipped_bad_base_qty:", skipped_bad_base_qty)


if __name__ == "__main__":
    main()
