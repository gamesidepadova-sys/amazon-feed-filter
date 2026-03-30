from datetime import date
import csv
import requests
import io
import re
import os
import pandas as pd

# =========================================================
# CONFIG
# =========================================================

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"
DAILY_DIR = "daily_snapshots"

os.makedirs(DAILY_DIR, exist_ok=True)

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0382", "0383"}
MIN_QTY = 10
MAX_DIFF_0373 = 20

SUPPLIER_WEIGHT = {
    "0372": 99.72,
    "0373": 99.73,
    "0374": 99.74,
    "0380": 99.80,
    "0381": 99.81,
    "0382": 99.82,
    "0383": 99.83
}

# =========================================================
# UTILS
# =========================================================

def today_str():
    return date.today().isoformat()

def today_tag(prefix):
    return f"{prefix}_{date.today().strftime('%Y%m%d')}"

def to_int(x, default=0):
    try:
        return int(float(str(x).replace(",", ".").strip()))
    except:
        return default

def to_float(x, default=0.0):
    try:
        return float(str(x).replace(",", ".").strip())
    except:
        return default

def supplier_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    return parts[1] if len(parts) >= 3 else ""

def valid_ean(ean: str) -> bool:
    return str(ean).isdigit()

# =========================================================
# SNAPSHOT ROBUSTO
# =========================================================

def get_all_snapshots():
    return sorted([
        f for f in os.listdir(DAILY_DIR)
        if f.startswith("snapshot_") and f.endswith(".csv")
    ])

def get_today_snapshot_name():
    return f"snapshot_{today_str()}.csv"

def is_first_run_today():
    return get_today_snapshot_name() not in get_all_snapshots()

def load_last_snapshot():
    files = get_all_snapshots()
    if not files:
        return None
    return pd.read_csv(f"{DAILY_DIR}/{files[-1]}", dtype={"ean": str})

def save_today_snapshot(df):
    df.to_csv(f"{DAILY_DIR}/{get_today_snapshot_name()}", index=False)

# =========================================================
# TAG LOGIC
# =========================================================

def detect_changes(today_df, yesterday_df):
    today_df["status"] = "UNCHANGED"
    today_df["stock_trend"] = "UNCHANGED"

    if yesterday_df is None:
        return today_df

    yesterday_df["quantita"] = yesterday_df["quantita"].apply(to_int)

    merged = today_df.merge(
        yesterday_df[["ean", "quantita"]],
        on="ean",
        how="left",
        suffixes=("", "_yesterday")
    )

    merged["quantita"] = merged["quantita"].apply(to_int)
    merged["quantita_yesterday"] = merged["quantita_yesterday"].apply(lambda x: to_int(x, 0))

    def calc(row):
        if pd.isna(row["quantita_yesterday"]):
            return ("NEW", "UNCHANGED")

        if row["quantita"] > 14 and row["quantita_yesterday"] <= 10:
            return ("UNCHANGED", "RECOVERED")

        if row["quantita"] > row["quantita_yesterday"]:
            return ("UNCHANGED", "INCREASED")

        return ("UNCHANGED", "UNCHANGED")

    merged[["status", "stock_trend"]] = merged.apply(
        lambda r: pd.Series(calc(r)), axis=1
    )

    return merged

def apply_tags(df):
    df["tag"] = ""

    for i, r in df.iterrows():
        tags = []

        if r["status"] == "NEW":
            tags.append(today_tag("new"))

        if r["stock_trend"] == "RECOVERED":
            tags.append(today_tag("mod"))

        df.at[i, "tag"] = ",".join(tags)

    return df

# =========================================================
# MAIN
# =========================================================

def main():
    print("📥 Scarico feed originale...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text), delimiter="|")

    rows = []
    for r in reader:
        try:
            sku = r.get("sku", "")
            supplier = supplier_from_sku(sku)

            if supplier not in ALLOWED_SUPPLIERS:
                continue

            qty = to_int(r.get("quantita"))
            if qty < MIN_QTY:
                continue

            ean = r.get("ean", "")
            if not valid_ean(ean):
                continue

            prezzo = to_float(r.get("prezzo_iva_esclusa"))
            sped = to_float(r.get("costo_spedizione"))

            row = dict(r)
            row["_supplier"] = supplier
            row["_price"] = prezzo + sped
            row["quantita"] = qty
            row["ean"] = str(ean)

            rows.append(row)

        except:
            continue

    # migliore per EAN
    df = pd.DataFrame(rows)

    best = []
    for ean, g in df.groupby("ean"):
        g = g.sort_values("_price")

        best_row = g.iloc[0]

        row_0373 = g[g["_supplier"] == "0373"]
        if not row_0373.empty:
            if row_0373.iloc[0]["_price"] <= best_row["_price"] + MAX_DIFF_0373:
                best_row = row_0373.iloc[0]

        best.append(best_row)

    today_df = pd.DataFrame(best)

    # ======================
    # SNAPSHOT + TAG
    # ======================

    yesterday_df = load_last_snapshot()

    print("📁 Snapshot presenti:", get_all_snapshots())
    print("📅 Oggi:", today_str())

    today_df = detect_changes(today_df, yesterday_df)

    if yesterday_df is None:
        print("🟡 Primo giorno → niente tag")
        today_df["tag"] = ""
        save_today_snapshot(today_df)

    elif is_first_run_today():
        print("🟢 Prima run del giorno → TAG ATTIVI")
        today_df = apply_tags(today_df)
        save_today_snapshot(today_df)

    else:
        print("⚪ Run successivo → niente tag")
        today_df["tag"] = ""

    # ======================
    # OUTPUT
    # ======================

    fields = list(today_df.columns)
    for col in ["_supplier", "_price", "status", "stock_trend", "quantita_yesterday"]:
        if col in fields:
            fields.remove(col)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter="|")
        writer.writeheader()

        for _, r in today_df.iterrows():
            supplier = r["_supplier"]
            peso_val = SUPPLIER_WEIGHT.get(supplier)

            if peso_val:
                r["peso"] = "24" + str(int(peso_val * 100))
            else:
                r["peso"] = "24"

            r_dict = r.to_dict()

            for col in ["_supplier", "_price", "status", "stock_trend", "quantita_yesterday"]:
                r_dict.pop(col, None)

            writer.writerow(r_dict)

    print("✅ Feed generato:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
