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
ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "clima e brico",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}
EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory", "montatura"}
MIN_QTY = 10
MAX_DIFF_0373 = 20

# ✅ PESO CORRETTO (COME VERSIONE ORIGINALE)
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

def today_tag(prefix):
    return f"{prefix}_{date.today().strftime('%Y%m%d')}"

def is_first_run_today():
    today = date.today().isoformat()
    return f"snapshot_{today}.csv" not in os.listdir(DAILY_DIR)

def no_snapshot_exists_yet():
    return len([f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")]) == 0

def save_daily_snapshot(df):
    today = date.today().isoformat()
    path = f"{DAILY_DIR}/snapshot_{today}.csv"

    # 🔒 salva supplier come stringa
    df["_supplier"] = df["_supplier"].astype(str)

    df.to_csv(path, index=False)

def load_yesterday_snapshot():
    files = sorted([f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")])

    if not files:
        print("⚠️ Nessuno snapshot precedente")
        return None

    path = f"{DAILY_DIR}/{files[-1]}"

    if os.path.getsize(path) == 0:
        print("⚠️ Snapshot vuoto")
        return None

    try:
        return pd.read_csv(path, dtype={"_supplier": str})
    except:
        print("⚠️ Errore lettura snapshot")
        return None

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
    parts = (sku or "").strip().split("_")
    if len(parts) >= 3:
        return parts[1]
    return ""

def norm(s: str) -> str:
    return str(s or "").strip().lower()

def clean_text(text: str) -> str:
    t = str(text or "")
    t = re.sub("<.*?>", " ", t)
    t = t.replace("&nbsp;", " ")
    t = t.replace('"', "")
    t = t.replace("|", " ")
    t = re.sub(r"\s+", " ", t)
    return t.strip()

def valid_ean(ean: str) -> bool:
    e = (ean or "").strip()
    return e.isdigit() and 8 <= len(e) <= 14

# =========================================================
# TAG LOGIC
# =========================================================

def detect_new(today_df, yesterday_df):
    if yesterday_df is None:
        today_df["status"] = "UNCHANGED"
        return today_df

    yesterday_eans = set(yesterday_df["ean"])
    today_df["status"] = today_df["ean"].apply(
        lambda e: "NEW" if e not in yesterday_eans else "UNCHANGED"
    )
    return today_df

def detect_stock_trend(today_df, yesterday_df):
    if yesterday_df is None:
        today_df["stock_trend"] = "UNCHANGED"
        return today_df

    merged = today_df.merge(
        yesterday_df[["ean", "quantita"]],
        on="ean",
        how="left",
        suffixes=("", "_yesterday")
    )

    def trend(row):
        if pd.isna(row["quantita_yesterday"]):
            return "UNCHANGED"

        # ✅ LOGICA CORRETTA: ieri < 10 e oggi > 14
        if row["quantita"] > 14 and row["quantita_yesterday"] < 10:
            return "RECOVERED"

        return "UNCHANGED"

    merged["stock_trend"] = merged.apply(trend, axis=1)
    return merged

def apply_tags(df):
    df["tag"] = ""
    new_tag = today_tag("new")
    mod_tag = today_tag("mod")

    for idx, row in df.iterrows():
        tags = []

        if row["status"] == "NEW":
            tags.append(new_tag)

        if row["stock_trend"] == "RECOVERED":
            tags.append(mod_tag)

        df.at[idx, "tag"] = ",".join(tags)

    return df

# =========================================================
# MAIN
# =========================================================

def main():
    print("📥 Scarico feed...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")

    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    reader.fieldnames = [f.replace("\ufeff", "") for f in reader.fieldnames]

    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso"
    ]

    rows_raw = list(reader)
    ean_groups = {}

    for r in rows_raw:
        try:
            sku = r.get("sku") or ""
            supplier = supplier_from_sku(sku)

            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = norm(r.get("cat1") or "")
            if cat1 not in ALLOWED_CAT1:
                continue

            titolo = norm(r.get("titolo_prodotto") or "")
            if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            qty = to_int(r.get("quantita"))
            if qty < MIN_QTY:
                continue

            ean = clean_text(r.get("ean") or "")
            if not valid_ean(ean):
                continue

            prezzo = to_float(r.get("prezzo_iva_esclusa"))
            sped = to_float(r.get("costo_spedizione"))
            prezzo_tot = prezzo + sped

            row = {k: clean_text(r.get(k) or "") for k in fields}
            row["_price"] = prezzo_tot
            row["_supplier"] = supplier
            row["_original_sku"] = sku

            ean_groups.setdefault(ean, []).append(row)

        except:
            continue

    best_by_ean = {}
    for ean, rows in ean_groups.items():
        min_row = min(rows, key=lambda x: x["_price"])
        min_price = min_row["_price"]

        row_0373 = next((r for r in rows if r["_supplier"] == "0373"), None)

        if row_0373 and row_0373["_price"] <= min_price + MAX_DIFF_0373:
            best = row_0373
        else:
            best = min_row

        best_by_ean[ean] = best

    today_df = pd.DataFrame(best_by_ean.values())
    yesterday_df = load_yesterday_snapshot()

    today_df = detect_new(today_df, yesterday_df)
    today_df = detect_stock_trend(today_df, yesterday_df)

    if no_snapshot_exists_yet():
        print("🟡 Primo snapshot (no tag)")
        today_df["tag"] = ""
        save_daily_snapshot(today_df)

    elif is_first_run_today():
        print("🟢 Primo run oggi → tag attivi")
        today_df = apply_tags(today_df)
        save_daily_snapshot(today_df)

    else:
        print("⚪ Run successivo → no tag")
        today_df["tag"] = ""

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields + ["tag"], delimiter="|")
        writer.writeheader()

        for _, r in today_df.iterrows():
            r_dict = r.to_dict()

            supplier_best = str(r_dict.get("_supplier"))

            peso_val = SUPPLIER_WEIGHT.get(supplier_best)
            if peso_val is not None:
                r_dict["peso"] = "24" + str(int(round(peso_val * 100)))
            else:
                r_dict["peso"] = "24"

            for col in ["_price", "_supplier", "_original_sku", "status", "stock_trend", "quantita_yesterday"]:
                r_dict.pop(col, None)

            writer.writerow(r_dict)

    print("✅ Feed generato correttamente")

if __name__ == "__main__":
    main()
