import csv
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_SELECTION = "selezione"
SHEET_SETTINGS = "settings"
INPUT_FILTERED = "filtered.csv"
SUPPLIERS_FILE = "suppliers.csv"


# ----------------------------
# Helpers
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
    # try key_<country> then key
    k1 = f"{key}_{country}".lower()
    for k, v in settings.items():
        if k.lower() == k1:
            return v
    for k, v in settings.items():
        if k.lower() == key.lower():
            return v
    return default


def detect_delim(first_line: str) -> str:
    if "\t" in first_line:
        return "\t"
    if "|" in first_line:
        return "|"
    if ";" in first_line:
        return ";"
    return ","


def supplier_code_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    return parts[1] if len(parts) >= 2 else ""


def _norm_bool(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y"}


def load_suppliers(path: str) -> Tuple[Dict[str, int], Dict[str, Decimal]]:
    """Return (handling_days_by_supplier, ship_cost_b2c_by_supplier)."""
    handling: Dict[str, int] = {}
    ship_cost: Dict[str, Decimal] = {}
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                code = (row.get("supplier_code") or "").strip()
                if not code:
                    continue

                active = _norm_bool(row.get("active"))
                if not active:
                    continue

                # handling = lead_b2c_max_days
                try:
                    handling[code] = int(str(row.get("lead_b2c_max_days") or "2").strip())
                except Exception:
                    handling[code] = 2

                # ship cost
                try:
                    ship_cost[code] = Decimal(str(row.get("ship_cost_b2c_eur") or "0").strip().replace(",", "."))
                except Exception:
                    ship_cost[code] = Decimal("0")
    except FileNotFoundError:
        pass

    return handling, ship_cost


# ----------------------------
# MAIN
# ----------------------------
def main():
    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID missing")

    country = os.environ.get("COUNTRY", "it").strip().lower()
    if country not in {"it", "de", "fr", "es"}:
        raise RuntimeError("COUNTRY must be one of: it,de,fr,es")

    out_b2c = f"amazon_{country}_b2c.csv"
    out_b2b = f"amazon_{country}_b2b.csv"
    out_listings = f"amazon_{country}_listings.json"
    out_priceinv = f"amazon_{country}_price_quantity.txt"  # legacy/debug

    # suppliers
    supplier_handling, supplier_ship = load_suppliers(SUPPLIERS_FILE)

    # Sheets client
    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    service = build("sheets", "v4", credentials=creds)

    # settings
    settings = kv_settings(read_sheet(service, spreadsheet_id, SHEET_SETTINGS))
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

    # selection
    sel_rows = read_sheet(service, spreadsheet_id, SHEET_SELECTION)
    if not sel_rows:
        raise RuntimeError('Sheet "selezione" empty or missing')

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

    # listings JSON feed skeleton
    listings = {
        "header": {
            "version": "2.0",
            "issueLocale": "it_IT" if country == "it" else "en_GB",
        },
        "messages": []
    }

    rows_b2c = rows_b2b = rows_listings = rows_priceinv = 0

    with open(INPUT_FILTERED, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)
        delim = detect_delim(first)
        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError("filtered.csv has no header")

        with open(out_b2c, "w", encoding="utf-8", newline="") as f1, \
             open(out_b2b, "w", encoding="utf-8", newline="") as f2, \
             open(out_priceinv, "w", encoding="utf-8", newline="") as f3:

            w1 = csv.DictWriter(f1, fieldnames=["sku", "price_b2c_eur", "qty_available", "country"])
            w2 = csv.DictWriter(f2, fieldnames=[
                "sku", "price_b2c_eur", "price_b2b_eur",
                "qty2_price_eur", "qty4_price_eur", "qty_available", "country"
            ])
            w1.writeheader()
            w2.writeheader()

            # legacy/debug txt
            w3 = csv.writer(f3, delimiter="\t", lineterminator="\n")
            w3.writerow(["sku", "price", "quantity", "fulfillment-channel", "handling-time"])

            msg_id = 1
            for row in reader:
                sku = (row.get("sku") or "").strip()
                if not sku:
                    continue
                if sku not in pub_b2c and sku not in pub_b2b:
                    continue

                base = to_dec(row.get("prezzo_iva_esclusa"))
                qty = to_int(row.get("quantita"), 0)
                if base <= 0 or qty < 0:
                    continue

                sup = supplier_code_from_sku(sku)
                ship = supplier_ship.get(sup, Decimal("0"))
                handling = supplier_handling.get(sup, 2)

                # prezzo: (base * markup + ship) * iva
                b2c = money((base * b2c_mul + ship) * vat_mul, round_decimals)

                # ---- B2C csv (debug/drive) ----
                if sku in pub_b2c:
                    w1.writerow({
                        "sku": sku,
                        "price_b2c_eur": f"{b2c}",
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2c += 1

                # ---- B2B csv (debug/drive) ----
                if sku in pub_b2b:
                    b2b = money(b2c * b2b_mul, round_decimals)
                    w2.writerow({
                        "sku": sku,
                        "price_b2c_eur": f"{b2c}",
                        "price_b2b_eur": f"{b2b}",
                        "qty2_price_eur": f"{money(b2c * qty2_mul, round_decimals)}",
                        "qty4_price_eur": f"{money(b2c * qty4_mul, round_decimals)}",
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2b += 1

                # ---- legacy/debug ----
                if sku in pub_b2c:
                    w3.writerow([sku, f"{b2c}", str(qty), "DEFAULT", str(handling)])
                    rows_priceinv += 1

                # ---- JSON_LISTINGS_FEED (price + qty) ----
                # NOTE: fulfillment_channel_code "DEFAULT" è quello che ti ha funzionato.
                # price patch: purchasable_offer -> our_price
                if sku in pub_b2c:
                    listings["messages"].append({
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
                    rows_listings += 1

    with open(out_listings, "w", encoding="utf-8") as fjson:
        json.dump(listings, fjson, ensure_ascii=False, indent=2)

    print(f"[{country}] Generated {out_b2c} rows={rows_b2c} (publish set size)")
    print(f"[{country}] Generated {out_b2b} rows={rows_b2b} (publish set size)")
    print(f"[{country}] Generated {out_priceinv} (legacy/debug)")
    print(f"[{country}] Generated {out_listings} messages={rows_listings}")
    print("Generated files:")
    os.system(f"ls -lh amazon_{country}_* || true")


if __name__ == "__main__":
    main()
