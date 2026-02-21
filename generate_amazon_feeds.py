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
            return field_map[c0]
    for k, real in field_map.items():
        for c in candidates:
            if c.lower().replace(" ", "_") in k:
                return real
    return None


def norm_sku(s: str) -> str:
    return clean_str(s)


# ----------------------------
# MAIN
# ----------------------------
def main():
    spreadsheet_id = clean_str(os.environ.get("GSHEET_ID"))
    if not spreadsheet_id:
        raise RuntimeError("GSHEET_ID missing")

    seller_id = clean_str(os.environ.get("AMAZON_SELLER_ID"))
    if not seller_id:
        raise RuntimeError("AMAZON_SELLER_ID missing")

    country = clean_str(os.environ.get("COUNTRY") or "it").lower()
    if country not in {"it", "de", "fr", "es"}:
        raise RuntimeError("COUNTRY must be one of: it,de,fr,es")

    out_b2c = f"amazon_{country}_b2c.csv"
    out_b2b = f"amazon_{country}_b2b.csv"
    out_priceinv = f"amazon_{country}_price_quantity.txt"
    out_listings = f"amazon_{country}_listings.json"

    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    sheets = build("sheets", "v4", credentials=creds)

    sel_tab = find_sheet_tab_case_insensitive(sheets, spreadsheet_id, SHEET_SELECTION)
    settings_tab = find_sheet_tab_case_insensitive(sheets, spreadsheet_id, SHEET_SETTINGS)
    suppliers_tab = find_sheet_tab_case_insensitive(sheets, spreadsheet_id, SHEET_SUPPLIERS)

    settings = kv_settings(read_sheet(sheets, spreadsheet_id, settings_tab))

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

    # ---- LOAD SUPPLIERS ----
    supplier_handling, supplier_ship_cost = load_supplier_sheet(
        sheets, spreadsheet_id, suppliers_tab
    )

    # ---- SELEZIONE ----
    sel_rows = read_sheet(sheets, spreadsheet_id, sel_tab)
    if not sel_rows:
        raise RuntimeError(f'Sheet "{sel_tab}" empty or missing')

    header = sel_rows[0]
    idx = build_index(header)

    def get_publish(row: List[str], key_candidates: List[str]) -> bool:
        for k in key_candidates:
            v = get_cell(row, idx, k, default="")
            if v != "":
                return norm_yes(v)
        for col_name in idx.keys():
            cn = col_name.replace(" ", "_")
            if "publish" in cn:
                for cand in key_candidates:
                    if cand.replace(" ", "_") in cn:
                        i = idx[col_name]
                        if i < len(row):
                            return norm_yes(row[i])
        return False

    pub_b2c, pub_b2b = set(), set()
    for r in sel_rows[1:]:
        sku = norm_sku(get_cell(r, idx, "sku"))
        if not sku:
            continue
        if get_publish(r, ["publish_b2c", "publish b2c", "b2c_publish", "publishb2c"]):
            pub_b2c.add(sku)
        if get_publish(r, ["publish_b2b", "publish b2b", "b2b_publish", "publishb2b"]):
            pub_b2b.add(sku)

    selected = pub_b2c | pub_b2b

    # ---- filtered.csv ----
    with open(INPUT_FILTERED, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)
        delim = detect_delim_from_first_line(first)

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError("filtered.csv has no header")

        field_map = normalize_fieldnames(reader.fieldnames)

        sku_field = pick_field(field_map, ["sku", "seller_sku", "item_sku", "merchant_sku", "sku_id"])
        qty_field = pick_field(field_map, ["quantita", "qty", "quantity", "stock"])
        base_field = pick_field(field_map, ["prezzo_iva_esclusa", "cost", "net_price", "base_price", "price"])

        if not sku_field:
            raise RuntimeError(f"SKU column not found in filtered.csv header: {reader.fieldnames}")

        skipped_missing_sku = 0
        skipped_not_selected = 0
        skipped_missing_fields = 0
        skipped_bad_base_qty = 0

        rows_b2c = 0
        rows_b2b = 0
        rows_priceinv = 0

        listings_messages: List[dict] = []
        msg_id = 1

        found_selected = set()

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
                sku = norm_sku(row.get(sku_field))
                if not sku:
                    skipped_missing_sku += 1
                    continue

                if sku not in selected:
                    skipped_not_selected += 1
                    continue

                found_selected.add(sku)

                if not qty_field or not base_field:
                    skipped_missing_fields += 1
                    continue

                raw_base = row.get(base_field)
                raw_qty = row.get(qty_field)
                if raw_base is None or raw_qty is None:
                    skipped_missing_fields += 1
                    continue

                base = to_dec(raw_base)
                qty = to_int(raw_qty)

                if base <= 0 or qty < 0:
                    skipped_bad_base_qty += 1
                    continue

                sup = sku.split("_")[1] if "_" in sku else ""
                ship = supplier_ship_cost.get(sup, Decimal("0"))
                handling = supplier_handling.get(sup, 2)

                b2c = money((base * b2c_mul + ship) * vat_mul, round_decimals)

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

                w3.writerow([sku, f"{b2c}", "", "", str(qty), "DEFAULT", str(handling)])
                rows_priceinv += 1

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

    missing = sorted(list(selected - found_selected))
    if FAIL_IF_NO_MATCH and len(found_selected) == 0 and len(selected) > 0:
        raise RuntimeError("No selected SKUs were found in filtered.csv")

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
    print(f"[{country}] Generated {out_priceinv} rows={rows_priceinv}")
    print(f"[{country}] Generated {out_listings} messages={len(listings_messages)}")


if __name__ == "__main__":
    main()
