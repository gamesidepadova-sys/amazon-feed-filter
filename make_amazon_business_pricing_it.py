#!/usr/bin/env python3
import csv
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, List

# Header ESATTO (come quello che hai incollato)
AMZ_HEADER: List[str] = [
    "sku",
    "price",
    "minimum-seller-allowed-price",
    "maximum-seller-allowed-price",
    "quantity",
    "handling-time",
    "fulfillment-channel",
    "business-price",
    "quantity-price-type",
    "quantity-lower-bound1",
    "quantity-price1",
    "quantity-lower-bound2",
    "quantity-price2",
    "quantity-lower-bound3",
    "quantity-price3",
    "quantity-lower-bound4",
    "quantity-price4",
    "quantity-lower-bound5",
    "quantity-price5",
    "progressive_discount_type",
    "progressive_discount_lower_bound1",
    "progressive_discount_value1",
    "progressive_discount_lower_bound2",
    "progressive_discount_value2",
    "progressive_discount_lower_bound3",
    "progressive_discount_value3",
    "national_stock_number",
    "unspsc_code",
    "pricing_action",
]

REQUIRED_IN = {"sku", "price_b2b_eur", "qty2_price_eur", "qty4_price_eur"}


def to_price_2dp(value: str) -> str:
    """Normalize to 2dp, dot decimal, no currency symbols."""
    s = (value or "").strip()
    if not s:
        raise ValueError("empty price")
    s = s.replace("€", "").replace(" ", "").replace(",", ".")
    try:
        d = Decimal(s)
    except InvalidOperation:
        raise ValueError(f"invalid price: {value!r}")
    if d <= 0:
        raise ValueError(f"non-positive price: {value!r}")
    d = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(d, "f")


def main(src_csv: str, out_txt: str) -> int:
    with open(src_csv, newline="", encoding="utf-8") as fin:
        r = csv.DictReader(fin)
        if not r.fieldnames:
            print(f"ERROR: {src_csv} has no header", file=sys.stderr)
            return 2

        missing = REQUIRED_IN - set(r.fieldnames)
        if missing:
            print(f"ERROR: input missing columns: {sorted(missing)}", file=sys.stderr)
            return 2

        rows_in = 0
        rows_out = 0
        skipped = 0

        with open(out_txt, "w", newline="", encoding="utf-8") as fout:
            w = csv.DictWriter(fout, fieldnames=AMZ_HEADER, delimiter="\t", lineterminator="\n")
            w.writeheader()

            for row in r:
                rows_in += 1
                sku = (row.get("sku") or "").strip()
                if not sku:
                    skipped += 1
                    continue

                try:
                    bprice = to_price_2dp(row.get("price_b2b_eur", ""))
                    q2 = to_price_2dp(row.get("qty2_price_eur", ""))
                    q4 = to_price_2dp(row.get("qty4_price_eur", ""))
                except ValueError:
                    skipped += 1
                    continue

                # Costruiamo una riga con TUTTE le colonne Amazon,
                # ma valorizziamo SOLO le colonne business/quantity discounts.
                out: Dict[str, str] = {k: "" for k in AMZ_HEADER}
                out["sku"] = sku

                # NON tocchiamo price/quantity standard
                out["business-price"] = bprice
                out["quantity-price-type"] = "fixed"

                out["quantity-lower-bound1"] = "2"
                out["quantity-price1"] = q2
                out["quantity-lower-bound2"] = "4"
                out["quantity-price2"] = q4

                # opzionale: fissiamo DEFAULT per coerenza (può anche restare vuoto)
                out["fulfillment-channel"] = "DEFAULT"

                w.writerow(out)
                rows_out += 1

    print(f"[make_amazon_business_pricing_it] in={rows_in} out={rows_out} skipped={skipped}")
    if rows_out == 0:
        print("WARNING: produced 0 rows (check publish_b2b / upstream filters).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: make_amazon_business_pricing_it.py <amazon_it_b2b.csv> <amazon_it_business_pricing.txt>", file=sys.stderr)
        sys.exit(1)
    sys.exit(main(sys.argv[1], sys.argv[2]))
