from datetime import datetim
import csv
import requests
import io
import re

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"
DEBUG_FILE = "debug_suppliers.csv"

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

    # Forziamo il delimitatore corretto
    reader = csv.DictReader(io.StringIO(text), delimiter="|")

    # FIX: rimuove BOM dal nome della prima colonna
    reader.fieldnames = [name.replace("\ufeff", "") for name in reader.fieldnames]

    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso","tag"
    ]

    # DIAGNOSTICA
    rows_raw = list(reader)

    count_total = len(rows_raw)
    count_supplier = 0
    count_cat1 = 0
    count_title = 0
    count_qty = 0
    count_ean = 0

    for r in rows_raw:
        sku = r.get("sku") or ""
        supplier = supplier_from_sku(sku)
        if supplier not in ALLOWED_SUPPLIERS:
            continue
        count_supplier += 1

        cat1 = norm(r.get("cat1") or r.get("categoria") or "")
        if cat1 not in ALLOWED_CAT1:
            continue
        count_cat1 += 1

        titolo = norm(r.get("titolo_prodotto") or r.get("nome") or "")
        if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
            continue
        count_title += 1

        qty = to_int(r.get("quantita") or r.get("qty"))
        if qty < MIN_QTY:
            continue
        count_qty += 1

        ean = clean_text(r.get("ean") or "")
        if not valid_ean(ean):
            continue
        count_ean += 1

    print("\n📊 DIAGNOSTICA FILTRI")
    print("Totale righe feed:", count_total)
    print("Dopo filtro supplier:", count_supplier)
    print("Dopo filtro cat1:", count_cat1)
    print("Dopo filtro titolo:", count_title)
    print("Dopo filtro qty:", count_qty)
    print("Dopo filtro ean:", count_ean)

    if count_ean == 0:
        raise Exception("❌ Feed vuoto dopo filtri — controlla la diagnostica sopra")

    # CREA DEBUG
    with open(DEBUG_FILE, "w", encoding="utf-8", newline="") as dbg:
        dbg_writer = csv.writer(dbg, delimiter="|")
        dbg_writer.writerow([
            "ean","sku_originale","supplier_sku",
            "supplier_best","price","tag_created"
        ])

    # RAGGRUPPAMENTO PER EAN
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
            row["tag"] = ""

            ean_groups.setdefault(ean, []).append(row)

        except:
            continue

    # SCELTA MIGLIORE PER EAN
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

    # SCRITTURA FILE FINALE
    today = datetime.now().strftime("%Y%m%d")

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields, delimiter="|", quoting=csv.QUOTE_NONE, escapechar="\\")

        writer.writeheader()

        for ean, r in best_by_ean.items():

            supplier_best = r["_supplier"]
            sku_originale = r["_original_sku"]
            supplier_sku = supplier_from_sku(sku_originale)

            tag_created = ""

            if supplier_sku and supplier_best != supplier_sku:
                tag_created = f"supplier_change_{supplier_best}_{today}"
                r["tag"] = tag_created
            else:
                r["tag"] = ""

            # DEBUG
            with open(DEBUG_FILE, "a", encoding="utf-8", newline="") as dbg:
                dbg_writer = csv.writer(dbg, delimiter="|")
                dbg_writer.writerow([
                    ean,
                    sku_originale,
                    supplier_sku,
                    supplier_best,
                    r["_price"],
                    tag_created
                ])

            # RIMUOVI CAMPI TECNICI
            r.pop("_price", None)
            r.pop("_supplier", None)
            r.pop("_original_sku", None)

            writer.writerow(r)

    print(f"\n📝 Feed generato: {OUTPUT_FILE}")
    print(f"📝 Debug generato: {DEBUG_FILE}")

if __name__ == "__main__":
    main()
