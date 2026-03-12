import os
import json
import csv
import io
import re
import requests
from concurrent.futures import ProcessPoolExecutor, as_completed

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"
PREVIOUS_FEED_FILE = "feed_poleepo_prev.json"

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
MAX_DIFF_0373 = 20  # differenza massima per preferire 0373

# -----------------------------
# Utility
# -----------------------------
def detect_delim(text: str) -> str:
    try:
        sample = text[:8192]
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return d.delimiter
    except Exception:
        first = text.splitlines()[0] if text else ""
        if "\t" in first: return "\t"
        if "|" in first: return "|"
        if ";" in first and first.count(";") > first.count(","): return ";"
        return ","

def to_int(x, default=0) -> int:
    try:
        s = str(x or "").strip()
        if not s: return default
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except Exception:
        return default

def to_float(x, default=0.0) -> float:
    try:
        s = str(x or "").strip()
        if not s: return default
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return default

def supplier_from_sku(sku: str) -> str:
    sku = sku or ""
    m = re.search(r"(03[0-9]{2})", sku)
    return m.group(1) if m else ""

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
    if not ean: return False
    e = ean.strip()
    return e.isdigit() and 8 <= len(e) <= 14

# -----------------------------
# Process batch
# -----------------------------
def process_batch(rows):
    # Raggruppa per EAN: lista di SKU candidati
    ean_groups = {}
    for r in rows:
        try:
            sku = r.get("sku") or r.get("SKU") or ""
            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS: continue

            cat1 = norm(r.get("cat1") or r.get("categoria") or "")
            if cat1 not in ALLOWED_CAT1: continue

            titolo = norm(r.get("titolo_prodotto") or r.get("nome") or "")
            if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS): continue

            qty = to_int(r.get("quantita") or r.get("qty"))
            if qty < MIN_QTY: continue

            ean = clean_text(r.get("ean") or "")
            if not valid_ean(ean): continue

            prezzo_raw = r.get("prezzo_iva_esclusa") or ""
            prezzo_num = to_float(prezzo_raw)
            spedizione = to_float(r.get("costo_spedizione"))
            prezzo_totale = prezzo_num + spedizione

            marca = clean_text(r.get("marca") or "")

            row_out = {k: clean_text(r.get(k) or "") for k in [
                "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
                "titolo_prodotto","immagine_principale","descrizione_prodotto",
                "costo_spedizione","cat2","cat3","marca","peso","tag"
            ]}
            row_out["sku"] = sku
            row_out["quantita"] = qty
            row_out["prezzo_iva_esclusa"] = clean_text(prezzo_raw)
            row_out["tag"] = f"supplier_{supplier}"
            row_out["ean"] = ean
            row_out["_price"] = prezzo_totale
            row_out["_marca"] = marca
            row_out["_supplier"] = supplier

            ean_groups.setdefault(ean, []).append(row_out)
        except Exception:
            continue

    # Ora scegli il miglior SKU per ogni EAN secondo la regola 0373
    best_ean_rows = {}
    for ean, rows in ean_groups.items():
        # Trova prezzo minimo
        min_price_row = min(rows, key=lambda x: x["_price"])
        price_min = min_price_row["_price"]

        # Trova eventuale 0373
        row_0373 = next((r for r in rows if r["_supplier"] == "0373"), None)

        if row_0373 and row_0373["_price"] <= price_min + MAX_DIFF_0373:
            chosen_row = row_0373
        else:
            chosen_row = min_price_row

        best_ean_rows[ean] = chosen_row

    return best_ean_rows

# -----------------------------
# Main
# -----------------------------
def main():
    print("📥 Scarico feed originale...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")
    delim = detect_delim(text)
    reader = list(csv.DictReader(io.StringIO(text), delimiter=delim))

    # Carica feed precedente
    previous_feed = {}
    if os.path.exists(PREVIOUS_FEED_FILE):
        with open(PREVIOUS_FEED_FILE, "r", encoding="utf-8") as f:
            previous_feed = json.load(f)

    # Batch
    batch_size = 2000
    batches = [reader[i:i+batch_size] for i in range(0, len(reader), batch_size)]

    best_by_ean = {}

    # Multiprocessing
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        for future in as_completed(futures):
            batch_result = future.result()
            best_by_ean.update(batch_result)

    if not best_by_ean:
        print("ℹ Nessuna modifica significativa rispetto al feed precedente.")
        return

    # Scrittura CSV con SKU storico come chiave
    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso","tag"
    ]
    modified_count = 0
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields, delimiter="|",
                                quoting=csv.QUOTE_NONE, escapechar="\\")
        writer.writeheader()
        snapshot = previous_feed.copy()
        for ean, row in best_by_ean.items():
            sku = row["sku"]  # mantieni SKU storico
            row_out = {k: v for k, v in row.items() if not k.startswith("_")}
            writer.writerow(row_out)
            # snapshot leggero: solo campi essenziali per incrementale
            snapshot[sku] = {
                "_price": row["_price"],
                "quantita": row["quantita"],
                "ean": row["ean"],
                "tag": row["tag"]
            }
            modified_count += 1

    print(f"\n📦 Prodotti modificati/nuovi: {modified_count}")
    print(f"📝 Feed generato correttamente: {OUTPUT_FILE}")

    # Salva snapshot aggiornato
    with open(PREVIOUS_FEED_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
