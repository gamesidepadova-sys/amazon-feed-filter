import csv
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Any, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_SELECTION = "selezione"   # case-insensitive match
SHEET_SETTINGS = "settings"     # case-insensitive match
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
        s = str(x).strip()
        if s == "":
            return default
        return int(s)
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


def detect_csv_delim_from_first_line(first_line: str) -> str:
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
def _get_sheet_tab_title_case_insensitive(service, spreadsheet_id: str, wanted: str) -> str:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    tabs = [s["properties"]["title"] for s in meta.get("sheets", []) if "properties" in s]
    if wanted in tabs:
        return wanted
    w = wanted.strip().lower()
    for t in tabs:
        if t.strip().lower() == w:
            return t
    # fallback: return wanted (will raise a clear error later)
    return wanted


def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
    real_tab = _get_sheet_tab_title_case_insensitive(service, spreadsheet_id, sheet_name)
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=real_tab
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


def load_suppliers(path: str) -> Tuple[Dict[str, int], Dict[str, Decimal]]:
    """
    Returns:
      handling_max_days_by_supplier_code
      ship_cost_b2c_eur_by_supplier_code
    """
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

                # handling
                try:
                    handling[code] = int(str(row.get("lead_b2c_max_days", "2")).strip() or "2")
                except Exception:
                    handling[code] = 2

                # ship cost
                try:
                    ship_cost[code] = Decimal(str(row.get("ship_cost_b2c_eur", "0")).strip().replace(",", ".") or "0")
                except Exception:
                    ship_cost[code] = Decimal("0")

    except FileNotFoundError:
        # optional file
        pass

    return handling, ship_cost


# ----------------------------
# MAIN
# ----------------------------
def main():
    spreadsheet_id = os.environ.get("GSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID missing")

    seller_id = os.environ.get("AMAZON_SELLER_ID", "").strip()
    if not seller_id:
        raise RuntimeError("AMAZON_SELLER_ID missing (sellerId required in listings feed)")

    country = os.environ.get("COUNTRY", "it").strip().lower()
    if country not in {"it", "de", "fr", "es"}:
        raise RuntimeError("COUNTRY must be one of: it,de,fr,es")

    out_b2c = f"amazon_{country}_b2c.csv"
    out_b2b = f"amazon_{country}_b2b.csv"
    out_listings = f"amazon_{country}_listings.json"

    # suppliers
    supplier_handling, supplier_ship_cost = load_suppliers(SUPPLIERS_FILE)

    # Sheets API
    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    sheets_service = build("sheets", "v4", credentials=creds)

    # settings
    settings = kv_settings(read_sheet(sheets_service, spreadsheet_id, SHEET_SETTINGS))

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

    # selection sheet
    sel_rows = read_sheet(sheets_service, spreadsheet_id, SHEET_SELECTION)
    if not sel_rows:
        raise RuntimeError('Selection sheet is empty or missing (tab "SELEZIONE")')

    sel_header = sel_rows[0]
    sel_idx = build_index(sel_header)

    pub_b2c: set[str] = set()
    pub_b2b: set[str] = set()

    for r in sel_rows[1:]:
        sku = get_cell(r, sel_idx, "sku").strip()
        if not sku:
            continue
        if norm_yes(get_cell(r, sel_idx, "publish_b2c")):
            pub_b2c.add(sku)
        if norm_yes(get_cell(r, sel_idx, "publish_b2b")):
            pub_b2b.add(sku)

    # DEBUG: selection parsing
    print("DEBUG selection_rows =", len(sel_rows))
    print("DEBUG selection_header =", sel_header)
    print("DEBUG pub_b2c size =", len(pub_b2c), "sample =", list(sorted(pub_b2c))[:10])
    print("DEBUG pub_b2b size =", len(pub_b2b), "sample =", list(sorted(pub_b2b))[:10])

    # filtered.csv
    with open(INPUT_FILTERED, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)
        delim = detect_csv_delim_from_first_line(first)
        reader = csv.DictReader(fin, delimiter=delim)

        if not reader.fieldnames:
            raise RuntimeError("filtered.csv has no header")

        # normalize fieldnames (for robustness)
        # We'll access by exact expected keys, but also try common variants.
        fields_lower = {f.strip().lower(): f for f in reader.fieldnames}

        def field(*cands: str) -> str:
            for c in cands:
                k = c.strip().lower()
                if k in fields_lower:
                    return fields_lower[k]
            return ""

        f_sku = field("sku")
        f_price = field("prezzo_iva_esclusa", "prezzo iva esclusa", "net_price", "price_net", "cost")
        f_qty = field("quantita", "qty", "quantity", "stock")

        if not f_sku:
            raise RuntimeError(f"filtered.csv: missing 'sku' column. Header={reader.fieldnames}")
        if not f_price:
            raise RuntimeError(f"filtered.csv: missing 'prezzo_iva_esclusa' (or alias) column. Header={reader.fieldnames}")
        if not f_qty:
            raise RuntimeError(f"filtered.csv: missing 'quantita' (or alias) column. Header={reader.fieldnames}")

        # outputs
        rows_b2c = 0
        rows_b2b = 0

        messages: List[Dict[str, Any]] = []
        message_id = 1

        # debug counters
        seen = 0
        skipped_not_selected = 0
        skipped_bad_values = 0
        matched_selected = 0

        with open(out_b2c, "w", encoding="utf-8", newline="") as f1, \
             open(out_b2b, "w", encoding="utf-8", newline="") as f2:

            w1 = csv.DictWriter(f1, fieldnames=["sku", "price_b2c_eur", "qty_available", "country"])
            w2 = csv.DictWriter(f2, fieldnames=[
                "sku", "price_b2c_eur", "price_b2b_eur",
                "qty2_price_eur", "qty4_price_eur", "qty_available", "country"
            ])
            w1.writeheader()
            w2.writeheader()

            for row in reader:
                seen += 1

                sku = (row.get(f_sku) or "").strip()
                if not sku:
                    continue

                if sku not in pub_b2c and sku not in pub_b2b:
                    skipped_not_selected += 1
                    continue

                base = to_dec(row.get(f_price))
                qty = to_int(row.get(f_qty), 0)

                # sanity
                if base <= 0 or qty < 0:
                    skipped_bad_values += 1
                    continue

                matched_selected += 1

                sup = supplier_code_from_sku(sku)
                ship = supplier_ship_cost.get(sup, Decimal("0"))
                handling = supplier_handling.get(sup, 2)

                # B2C price: (base * markup + ship) * vat
                b2c = money((base * b2c_mul + ship) * vat_mul, round_decimals)

                # Build one listings PATCH that sets:
                # - quantity (MFN)
                # - purchasable offer (price)
                # For SKU in either B2C or B2B, we still set offer+qty (B2C price is the consumer price here).
                patches = [
                    {
                        "op": "replace",
                        "path": "/attributes/fulfillment_availability",
                        "value": [
                            {
                                # IMPORTANT: for your account, previous test accepted "DEFAULT".
                                # We keep "DEFAULT" (not "MFN") to avoid the earlier error.
                                "fulfillment_channel_code": "DEFAULT",
                                "quantity": qty
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
                                                "value_with_tax": float(b2c)
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]

                messages.append({
                    "messageId": message_id,
                    "sku": sku,
                    "operationType": "PATCH",
                    "productType": "PRODUCT",
                    "patches": patches
                })
                message_id += 1

                if sku in pub_b2c:
                    w1.writerow({
                        "sku": sku,
                        "price_b2c_eur": str(b2c),
                        "qty_available": qty,
                        "country": country
                    })
                    rows_b2c += 1

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

    # write listings json
    out_obj = {
        "header": {
            "sellerId": seller_id,
            "version": "2.0",
            "issueLocale": f"{country}_IT" if country == "it" else "en_US"
        },
        "messages": messages
    }
    # keep it_IT for Italy explicitly
    if country == "it":
        out_obj["header"]["issueLocale"] = "it_IT"

    with open(out_listings, "w", encoding="utf-8") as f:
        json.dump(out_obj, f, ensure_ascii=False, indent=2)

    print(f"[{country}] Generated {out_b2c} rows={rows_b2c}")
    print(f"[{country}] Generated {out_b2b} rows={rows_b2b}")
    print(f"[{country}] Generated {out_listings} messages={len(messages)}")

    # DEBUG summary
    print("DEBUG filtered rows seen =", seen)
    print("DEBUG matched_selected =", matched_selected)
    print("DEBUG skipped_not_selected =", skipped_not_selected)
    print("DEBUG skipped_bad_values =", skipped_bad_values)

    # EXTRA: if empty, print a hint
    if len(messages) == 0:
        print("DEBUG: messages is empty. Most common causes:")
        print(" - pub_b2c/pub_b2b empty (publish_b2c/publish_b2b not 'YES')")
        print(" - SKU mismatch between selection and filtered.csv (spaces/hidden chars)")
        print(" - base/qty parsed as 0 due to wrong column names or formatting")


if __name__ == "__main__":
    main()
