from datetime import datetime
import csv
import requests
import io
import re

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

# Peso per identificare il fornitore vincente
SUPPLIER_WEIGHT = {
    "0372": 99.72,
    "0373": 99.73,
    "0374": 99.74,
    "0380": 99.80,
    "0381": 99.81,
    "0382": 99.82,
    "0383": 99.83
}

def to_int(x, default=0):
    try:
        s = str(x or "").strip()
        if not s:
            return default
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except:
        return default

def to_float(x, default=0.0):
    try:
        s = str(x or "").strip()
        if not s:
            return default
        s = s.replace(",", ".")
        return float(s)
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
    t = t.replace("\n", " ")
    t = t.replace("\r", " ")
    t = re.sub(" +", " ", t)
    return t.strip()

def valid_ean(ean: str) -> bool:
    e = (ean or "").strip()
    return e.isdigit() and 8 <= len(e) <= 14

def main():
    print("📥 Scarico feed originale...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")

    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    reader.fieldnames = [name.replace("\ufeff", "") for name in reader.fieldnames]

    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso"
    ]

    rows_raw = list(reader)

    # ==========================
    # RAGGRUPPAMENTO PER EAN
    # ==========================

    ean_groups = {}

    for r in rows_raw:
        try:
            sku = r.get("sku") or ""
            supplier = supplier_from_sku(sku)

            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = norm(r.get("cat1") or r.get("categoria") or "")
            if cat1 not in ALLOWED_CAT1:
                continue

            titolo = norm(r.get("titolo_prodotto") or r.get("nome") or "")
            if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            qty = to_int(r.get("quantita") or r.get("qty"))
            if qty < MIN_QTY:
                continue

            ean = clean_text(r.get("ean") or "")
            if not valid_ean(ean):
                continue

            prezzo = to_float(r.get("prezzo_iva_esclusa"))
            spedizione = to_float(r.get("costo_spedizione"))
            prezzo_totale = prezzo + spedizione

            row = {k: clean_text(r.get(k) or "") for k in fields}
            row["_original_sku"] = sku
            row["_price"] = prezzo_totale
            row["_supplier"] = supplier

            ean_groups.setdefault(ean, []).append(row)

        except:
            continue

    # ==========================
    # SCELTA MIGLIORE PER EAN
    # ==========================

    best_by_ean = {}

    for ean, rows in ean_groups.items():

        min_row = min(rows, key=lambda x: x["_price"])
        min_price = min_row["_price"]

        row_0373 = next((r for r in rows if r["_supplier"] == "0373"), None)

        if row_0373 and row_0373["_price"] <= min_price + MAX_DIFF_0373:
            best_row = row_0373
        else:
            best_row = min_row

        best_by_ean[ean] = best_row

    # ==========================
    # SCRITTURA FILE FINALE
    # ==========================

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out,
            fieldnames=fields,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            escapechar="\\"
        )

        writer.writeheader()

        for ean, r in best_by_ean.items():

            supplier_best = r["_supplier"]

            # ==========================
            # NUOVA LOGICA PESO 24XXXXX
            # ==========================

            peso_val = SUPPLIER_WEIGHT.get(supplier_best)

            if peso_val is not None:
                r["peso"] = "24" + str(int(round(float(peso_val) * 100)))
            else:
                r["peso"] = "24"

            # pulizia campi tecnici
            r.pop("_price", None)
            r.pop("_supplier", None)
            r.pop("_original_sku", None)

            writer.writerow(r)

    print(f"\n📝 Feed generato: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
