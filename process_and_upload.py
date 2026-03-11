import csv
import requests
import io
import re
from collections import defaultdict

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0382", "0383"}
ALLOWED_CAT1 = {"informatica","audio e tv","clima e brico","consumabili e ufficio","salute, beauty e fitness"}
EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory","montatura"}
MIN_QTY = 10
MIN_OPTIMIZED_STOCK = 16
MAX_PRICE_DIFF = 40  # per scartare prodotti troppo cari

# -----------------------------
# Funzioni di utilità
# -----------------------------
def detect_delim(text: str) -> str:
    try:
        d = csv.Sniffer().sniff(text[:8192], delimiters=",;\t|")
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
        return int(float(s.replace(",", ".")))
    except Exception:
        return default

def supplier_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""

def norm(s: str) -> str:
    return str(s or "").strip().lower()

def clean_text(text: str) -> str:
    t = str(text or "")
    t = re.sub("<.*?>", " ", t)
    t = t.replace("&nbsp;", " ").replace('"', "").replace("|"," ").replace("\n"," ").replace("\r"," ")
    t = re.sub(" +", " ", t)
    return t.strip()

# -----------------------------
# Funzione scelta prodotto migliore
# -----------------------------
def choose_product(rows):
    if not rows: return None
    if len(rows) == 1: return rows[0]

    rows_sorted = sorted(rows, key=lambda x: x["price"])
    lowest = rows_sorted[0]["price"]
    second_lowest = rows_sorted[1]["price"] if len(rows_sorted) > 1 else lowest

    # Rimuovi prodotti troppo cari
    rows = [r for r in rows if r["price"] <= lowest + MAX_PRICE_DIFF and r["qty"] >= MIN_OPTIMIZED_STOCK]
    if not rows: return None

    # 1️⃣ Priorità 0382
    for r in rows:
        if r["supplier"] == "0382" and r["qty"] >= MIN_OPTIMIZED_STOCK and r["price"] == lowest:
            return r

    # 2️⃣ Priorità 0381
    for r in rows:
        if r["supplier"] == "0381" and r["qty"] >= MIN_OPTIMIZED_STOCK and r["price"] == lowest:
            return r

    # 3️⃣ Priorità 0373 fino a +20€ rispetto al secondo prezzo più basso
    for r in rows:
        if r["supplier"] == "0373" and r["qty"] >= MIN_OPTIMIZED_STOCK and r["price"] <= second_lowest + 20:
            return r

    # 4️⃣ 0372 o 0380 → prezzo più basso
    candidates = [r for r in rows if r["supplier"] in {"0372","0380"} and r["qty"] >= MIN_OPTIMIZED_STOCK]
    if candidates:
        return sorted(candidates, key=lambda x: x["price"])[0]

    # 5️⃣ 0383 solo se unico
    if len(rows) == 1 and rows[0]["supplier"] == "0383":
        return rows[0]

    # fallback ultima scelta
    return rows_sorted[0]

# -----------------------------
# Main
# -----------------------------
def main():
    print("📥 Scarico feed originale...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")
    delim = detect_delim(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    fields = ["cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
              "titolo_prodotto","immagine_principale","descrizione_prodotto",
              "costo_spedizione","cat2","cat3","marca","peso"]

    products_by_ean = defaultdict(list)
    error_rows = []

    for i, r in enumerate(reader,1):
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

            prezzo = float(str(r.get("prezzo_iva_esclusa") or "0").replace(",", "."))
            ean = r.get("ean") or ""

            products_by_ean[ean].append({"raw": r,"sku":sku,"supplier":supplier,"qty":qty,"price":prezzo})
        except Exception as e:
            error_rows.append((i,str(e)))

    if not products_by_ean: raise Exception("❌ Feed vuoto dopo filtri!")

    rows_out = []
    for ean, rows in products_by_ean.items():
        chosen = choose_product(rows)
        if not chosen: continue
        r = chosen["raw"]
        row = {k: clean_text(r.get(k)) if k != "quantita" else chosen["qty"] for k in fields}
        rows_out.append(row)

    print(f"✅ Prodotti finali: {len(rows_out)} / EAN totali: {len(products_by_ean)}")
    if error_rows: print(f"⚠ Righe con errore: {len(error_rows)} (es. riga {error_rows[0][0]})")

    with open(OUTPUT_FILE,"w",encoding="utf-8",newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields, delimiter="|", quoting=csv.QUOTE_NONE, escapechar="\\")
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)

    print(f"📝 Feed ottimizzato generato: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
