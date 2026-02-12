import csv
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any, Set

from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_SELECTION = "selezione"
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


def detect_delim_from_first_line(first_line: str) -> str:
    if "\t" in first_line:
        return "\t"
    if "|" in first_line:
        return "|"
    if ";" in first_line:
        return ";"
    return ","


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
    out: Dict[str, int] = {}
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
    out: Dict[str, Decimal] = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                code = (row.get("supplier_code") or "").strip()
                if not code:
                    continue
                try:
                    out[code] = Decimal(str(row.get("ship_cost_b2c_eur", "0")).strip().replace(",", "."))
                except Exception:
                    out[code] = Decimal("0")
    except FileNotFoundError:
        pass
    return out


# ----------------------------
# Locale helpers
# ----------------------------
def issue_locale_for_country(country: str) -> str:
    return {
        "it": "it_IT",
        "de": "de_DE",
        "fr": "fr_FR",
        "es": "es_ES",
    }.get(country.lower(), "en_US")


def currency_for_country(country: str) -> str:
    # per i mercati EU che stai usando ora è EUR
    return "EUR"


# ----------------------------
# MAIN
# ----------------------------
def main():
    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID missing")

    country = os.environ.get("COUNTRY", "it").strip().lower()

    # ✅ obbligatorio per JSON_LISTINGS_FEED
    seller_id = os.environ.get("AMAZON_SELLER_ID", "").strip()
    if not seller_id:
        raise RuntimeError("AMAZON_SELLER_ID missing (set GitHub secret to Merchant Token / Seller ID)")

    out_b2c = f"amazon_{country}_b2c.csv"
    out_b2b = f"amazon_{country}_b2b.csv"
    out_priceinv = f"amazon_{country}_price_quantity.txt"   # legacy/debug
    out_listings = f"amazon_{country}_listings.json"        # SP-API JSON_LISTINGS_FEED

    # suppliers
    supplier_handling = load_supplier_handling_max_days(SUPPLIERS_FILE)
    supplier_ship_cost = load_supplier_ship_cost_b2c(SUPPLIERS_FILE)

    # Google Sheets client
    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    sheets = build("sheets", "v4", credentials=creds)

    settings = kv_settings(read_sheet(sheets, spreadsheet_id, SHEET_SETTINGS))

    # settings
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

    default_product_type = (get_setting(settings, "default_product_type", country, "PRODUCT").strip() or "PRODUCT")
    issue_locale = issue_locale_for_country(country)
    currency = currency_for_country(country)

    # ---- selezione ----
    sel_rows = read_sheet(sheets, spreadsheet_id, SHEET_SELECTION)
    if not sel_rows:
        raise RuntimeError('Sheet "selezione" empty or missing')

    sel_idx = build_index(sel_rows[0])

    pub_b2c: Set[str] = set()
    pub_b2b: Set[str] = set()

    for r in sel_rows[1:]:
        sku = get_cell(r, sel_idx, "sku")
        if not sku:
            continue
        if norm_yes(get_cell(r, sel_idx, "publish_b2c")):
            pub_b2c.add(sku)
        if norm_yes(get_cell(r, sel_idx, "publish_b2b")):
            pub_b2b.add(sku)

    publish_set = pub_b2c | pub_b2b

    # ---- filtered.csv ----
    with open(INPUT_FILTERED, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)
        delim = detect_delim_from_first_line(first)

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError("filtered.csv has no header")

        # tracking per miglioria 6
        all_filtered_skus: Set[str] = set()

        # output writers
        rows_b2c = 0
        rows_b2b = 0
        rows_priceinv = 0

        listings_messages: List[dict] = []
        message_id = 1

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
                    continue

                all_filtered_skus.add(sku)

                # se non è selezionato per publish, ignora
                if sku not in publish_set:
                    continue

                base = to_dec(row.get("prezzo_iva_esclusa"))
                qty = to_int(row.get("quantita"), 0)

                # sanity checks
                if base <= 0 or qty < 0:
                    continue

                sup = supplier_code_from_sku(sku)
                ship = supplier_ship_cost.get(sup, Decimal("0"))
                handling = supplier_handling.get(sup, 2)

                # ✅ prezzo finale B2C: ((base*markup)+ship) * IVA
                b2c = money((base * b2c_mul + ship) * vat_mul, round_decimals)

                if sku in pub_b2c:
                    w1.writerow({
                        "sku": sku,
                        "price_b2c_eur": str(b2c),
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2c += 1

                    # legacy/debug
                    w3.writerow([sku, str(b2c), "", "", qty, "MFN", handling])
                    rows_priceinv += 1

                if sku in pub_b2b:
                    b2b = money(b2c * b2b_mul, round_decimals)
                    q2 = money(b2c * qty2_mul, round_decimals)
                    q4 = money(b2c * qty4_mul, round_decimals)

                    w2.writerow({
                        "sku": sku,
                        "price_b2c_eur": str(b2c),
                        "price_b2b_eur": str(b2b),
                        "qty2_price_eur": str(q2),
                        "qty4_price_eur": str(q4),
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2b += 1

                # ✅ JSON_LISTINGS_FEED: qty + prezzo (DEFAULT, non MFN)
                listings_messages.append({
                    "messageId": message_id,
                    "sku": sku,
                    "operationType": "PATCH",
                    "productType": default_product_type,
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
                                "currency": currency,
                                "our_price": [{
                                    "schedule": [{
                                        "value_with_tax": float(b2c)
                                    }]
                                }]
                            }]
                        }
                    ]
                })
                message_id += 1

        # ----------------------------
        # MIGLIORIA 6:
        # SKU in Selezione ma NON più in filtered -> force qty=0
        # ----------------------------
        missing_skus = publish_set - all_filtered_skus
        for sku in sorted(missing_skus):
            listings_messages.append({
                "messageId": message_id,
                "sku": sku,
                "operationType": "PATCH",
                "productType": default_product_type,
                "patches": [
                    {
                        "op": "replace",
                        "path": "/attributes/fulfillment_availability",
                        "value": [{
                            "fulfillment_channel_code": "DEFAULT",
                            "quantity": 0
                        }]
                    }
                ]
            })
            message_id += 1

        # write listings json
        listings_payload = {
            "header": {
                "sellerId": seller_id,
                "version": "2.0",
                "issueLocale": issue_locale
            },
            "messages": listings_messages
        }

        with open(out_listings, "w", encoding="utf-8") as fj:
            json.dump(listings_payload, fj, ensure_ascii=False, indent=2)

    print(f"[{country}] Generated {out_b2c} rows={rows_b2c} (publish set size={len(publish_set)})")
    print(f"[{country}] Generated {out_b2b} rows={rows_b2b} (publish set size={len(publish_set)})")
    print(f"[{country}] Generated {out_priceinv} (legacy/debug)")
    print(f"[{country}] Generated {out_listings} messages={len(listings_messages)}")


if __name__ == "__main__":
    main()
