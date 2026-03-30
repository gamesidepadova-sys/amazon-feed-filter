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

def snapshot_path():
    return f"{DAILY_DIR}/snapshot_{date.today().isoformat()}.csv"

def is_first_run_today():
    return not os.path.exists(snapshot_path())

def load_yesterday_snapshot():
    files = sorted(os.listdir(DAILY_DIR))
    if not files:
        return None

    path = f"{DAILY_DIR}/{files[-1]}"

    try:
        df = pd.read_csv(path, dtype={"ean": str})
        df["quantita"] = df["quantita"].astype(int)
        return df
    except:
        return None

def save_snapshot(df):
    snap = df[["ean", "quantita"]].copy()
    snap.to_csv(snapshot_path(), index=False)

def to_int(x, default=0):
    try:
        s = str(x or "").replace(".", "").replace(",", ".")
        return int(float(s))
    except:
        return default

def to_float(x, default=0.0):
    try:
        s = str(x or "").replace(",", ".")
        return float(s)
    except:
        return default

def supplier_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    return parts[1] if len(parts) >= 3 else ""

def norm(s: str) -> str:
    return str(s or "").strip().lower()

def clean_text(text: str) -> str:
    t = str(text or "")
    t = re.sub("<.*?>", " ", t)
    t = t.replace("&nbsp;", " ")
    t = t.replace('"', "")
    t = t.replace("|", " ")
    t = t.replace("\n", " ")
    t = t.replace("\r", " ")
    return re.sub(" +", " ", t).strip()

def valid_ean(ean: str) -> bool:
    return str(ean).isdigit()

# =========================================================
# MAIN
# =========================================================

def main():

    print("📥 Scarico feed originale...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()

    text = resp.content.decode("utf-8-sig", errors="replace")

    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    reader.fieldnames = [f.replace("\ufeff", "") for f in reader.fieldnames]

    rows = []

    for r in reader:
        try:
            sku = r.get("sku", "")
            supplier = supplier_from_sku(sku)

            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = norm(r.get("cat1"))
            if cat1 not in ALLOWED_CAT1:
                continue

            titolo = norm(r.get("titolo_prodotto"))
            if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            qty = to_int(r.get("quantita"))
            if qty < MIN_QTY:
                continue

            ean = clean_text(r.get("ean"))
            if not valid_ean(ean):
                continue

            prezzo = to_float(r.get("prezzo_iva_esclusa"))
            sped = to_float(r.get("costo_spedizione"))

            rows.append({
                "ean": ean,
                "sku": sku,
                "supplier": supplier,
                "quantita": qty,
                "price": prezzo + sped,
                "titolo_prodotto": clean_text(r.get("titolo_prodotto")),
                "cat1": clean_text(r.get("cat1")),
                "marca": clean_text(r.get("marca")),
                "peso": ""
            })

        except:
            continue

    df = pd.DataFrame(rows)

    # =========================================================
    # BEST PER EAN (VELOCE)
    # =========================================================

    df = df.sort_values("price")

    best = df.groupby("ean", as_index=False).first()

    # preferenza 0373
    df_0373 = df[df["supplier"] == "0373"]

    merged = best.merge(df_0373[["ean", "price"]], on="ean", how="left", suffixes=("", "_0373"))

    mask = (merged["price_0373"].notna()) & (merged["price_0373"] <= merged["price"] + MAX_DIFF_0373)

    best.loc[mask, "supplier"] = "0373"

    today_df = best.copy()

    # =========================================================
    # SNAPSHOT + TAG
    # =========================================================

    yesterday = load_yesterday_snapshot()

    today_df["status"] = "UNCHANGED"
    today_df["stock_trend"] = "UNCHANGED"

    if yesterday is not None:

        merged = today_df.merge(yesterday, on="ean", how="left", suffixes=("", "_y"))

        merged["quantita_y"] = merged["quantita_y"].fillna(0).astype(int)

        merged.loc[merged["quantita_y"] == 0, "status"] = "NEW"

        merged.loc[
            (merged["quantita"] > 14) &
            (merged["quantita_y"] <= 10),
            "stock_trend"
        ] = "RECOVERED"

        merged.loc[
            (merged["quantita"] > merged["quantita_y"]) &
            (merged["stock_trend"] != "RECOVERED"),
            "stock_trend"
        ] = "INCREASED"

        today_df = merged

    # =========================================================
    # TAG SOLO PRIMA RUN
    # =========================================================

    if is_first_run_today():
        print("🟢 Primo run → tag attivi")

        new_tag = today_tag("new")
        mod_tag = today_tag("mod")

        today_df["tag"] = ""

        today_df.loc[today_df["status"] == "NEW", "tag"] = new_tag

        today_df.loc[
            today_df["stock_trend"].isin(["RECOVERED", "INCREASED"]),
            "tag"
        ] = mod_tag

        save_snapshot(today_df)

    else:
        print("⚪ Run successivo → niente tag")
        today_df["tag"] = ""

    # =========================================================
    # PESO (CORRETTO)
    # =========================================================

    today_df["peso"] = today_df["supplier"].map(
        lambda s: "24" + str(int(SUPPLIER_WEIGHT[s] * 100))
        if s in SUPPLIER_WEIGHT else "24"
    )

    # =========================================================
    # CLEAN OUTPUT
    # =========================================================

    today_df = today_df.drop(columns=[
        "supplier", "price", "status", "stock_trend", "quantita_y"
    ], errors="ignore")

    # =========================================================
    # EXPORT
    # =========================================================

    today_df.to_csv(
        OUTPUT_FILE,
        index=False,
        sep="|",
        quoting=csv.QUOTE_NONE,
        escapechar="\\"
    )

    print(f"\n📝 Feed generato: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
