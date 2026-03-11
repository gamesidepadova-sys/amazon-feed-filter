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
MAX_PRICE_DIFF = 40
PRICE_SIMILARITY_THRESHOLD = 0.02  # 2 cent per considerare prezzi quasi uguali

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
    min_price = rows_sorted[0]["price"]

    # Rimuovi SKU troppo cari
    rows = [r for r in rows if r["price"] <= min_price + MAX_PRICE_DIFF and r["qty"] >= MIN_OPTIMIZED_STOCK]
    if not rows: return None

    # Raggruppa prezzi simili per considerare SKU quasi uguali
    price_groups = defaultdict(list)
    for r in rows:
        key = round(r["price"],2)  # arrotonda a centesimi
        price_groups[key].append(r)
    lowest_group = price_groups[min(price_groups.keys())]

    # Priorità dinamica fornitori
    for sup in ["0382","0372","0380","0381","0373"]:
        candidates = [r for r in lowest_group if r["supplier"] == sup]
        if candidates:
            # Preferisce marche conosciute
            known = [r for r in candidates if r["raw"].get("marca","").strip().upper() not in {"ND",""}]
            return known[0] if known else candidates[0]

    # fallback ultima scelta
    return lowest_group[0]

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
