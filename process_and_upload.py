import csv
import requests
import io
import re
import sys
from collections import defaultdict

csv.field_size_limit(sys.maxsize)

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"

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


# -----------------------------
# Utility
# -----------------------------
def detect_delim(text: str) -> str:
    try:
        return csv.Sniffer().sniff(text[:8192], delimiters=",;\t|").delimiter
    except Exception:
        return ";"  # più stabile per feed europei


def to_int(x):
    try:
        return int(float(str(x).replace(".", "").replace(",", ".")))
    except:
        return 0


def to_float(x):
    try:
        return float(str(x).replace(",", "."))
    except:
        return 0.0


def supplier_from_sku(sku: str) -> str:
    m = re.search(r"(03[0-9]{2})", sku or "")
    return m.group(1) if m else ""


def norm(s: str) -> str:
    return str(s or "").strip().lower()


def clean_text(text: str) -> str:
    t = str(text or "")
    t = re.sub("<.*?>", " ", t)
    t = t.replace("&nbsp;", " ")
    t = t.replace('"', "")
    t = re.sub(" +", " ", t)
    return t.strip()


def valid_ean(ean: str) -> bool:
    e = (ean or "").strip()
    return e.isdigit() and 8 <= len(e) <= 14


# -----------------------------
# MAIN
# -----------------------------
def main():

    print("📥 Scarico feed...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")

    delim = detect_delim(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso"
    ]

    ean_groups = defaultdict(list)

    # -----------------------------
    # 1️⃣ Filtri e raggruppamento
    # -----------------------------
    for r in reader:

        try:
            sku = r.get("sku") or ""
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
            spedizione = to_float(r.get("costo_spedizione"))
            prezzo_totale = prezzo + spedizione

            row = {k: clean_text(r.get(k)) for k in fields}
            row["quantita"] = qty
            row["_price"] = prezzo_totale
            row["_supplier"] = supplier

            ean_groups[ean].append(row)

        except:
            continue

    if not ean_groups:
        raise Exception("❌ Nessun prodotto valido")

    # -----------------------------
    # 2️⃣ Scelta miglior fornitore
    # -----------------------------
    best_supplier_by_ean = {}

    for ean, rows in ean_groups.items():

        min_row = min(rows, key=lambda x: x["_price"])
        min_price = min_row["_price"]

        row_0373 = next((r for r in rows if r["_supplier"] == "0373"), None)

        if row_0373 and row_0373["_price"] <= min_price + MAX_DIFF_0373:
            best_supplier_by_ean[ean] = row_0373
        else:
            best_supplier_by_ean[ean] = min_row

    # -----------------------------
    # 3️⃣ Scrittura file finale
    # -----------------------------
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:

        writer = csv.DictWriter(
            out,
            fieldnames=fields,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            escapechar="\\"
        )

        writer.writeheader()

        for ean, rows in ean_groups.items():

            best = best_supplier_by_ean[ean]

            for r in rows:

                out_row = r.copy()

                # aggiorna prezzo e quantità
                out_row["quantita"] = best["quantita"]
                out_row["prezzo_iva_esclusa"] = best.get("prezzo_iva_esclusa", "")

                # 🔥 categoria con fornitore
                base_cat = r["cat1"]
                out_row["cat1"] = f"{base_cat}_{best['_supplier']}"

                # rimuove campi interni
                out_row.pop("_price", None)
                out_row.pop("_supplier", None)

                writer.writerow(out_row)

    print(f"\n📝 Feed generato correttamente: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
