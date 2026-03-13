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
MAX_DIFF_0373 = 20  # regola preferenza 0373

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
        if not s:
            return default
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except Exception:
        return default

def to_float(x, default=0.0) -> float:
    try:
        s = str(x or "").strip()
        if not s:
            return default
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
    if not ean:
        return False
    e = ean.strip()
    return e.isdigit() and 8 <= len(e) <= 14

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

    # -----------------------------
    # Raggruppa tutti i prodotti per EAN
    # -----------------------------
    ean_groups = {}
    for r in reader:
        try:
            sku = r.get("sku") or r.get("SKU") or ""
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

            prezzo_raw = r.get("prezzo_iva_esclusa") or ""
            prezzo_num = to_float(prezzo_raw)
            spedizione = to_float(r.get("costo_spedizione"))
            prezzo_totale = prezzo_num + spedizione  # prezzo reale

            row = {
                "sku": sku,
                "ean": ean,
                "quantita": qty,
                "prezzo_iva_esclusa": prezzo_raw,
                "tag": f"supplier_{supplier}",
                "_price": prezzo_totale,
                "_supplier": supplier
            }

            ean_groups.setdefault(ean, []).append(row)

        except Exception as e:
            print(f"⚠ Errore riga: {e}")

    if not ean_groups:
        raise Exception("❌ Nessun prodotto valido dopo filtri!")

    # -----------------------------
    # Seleziona lo SKU migliore per ogni EAN
    # -----------------------------
    best_by_ean = {}
    for ean, rows in ean_groups.items():
        # Trova prezzo minimo
        min_price_row = min(rows, key=lambda x: x["_price"])
        price_min = min_price_row["_price"]

        # Controlla se 0373 entro MAX_DIFF_0373
        row_0373 = next((r for r in rows if r["_supplier"] == "0373"), None)
        if row_0373 and row_0373["_price"] <= price_min + MAX_DIFF_0373:
            best_row = row_0373
        else:
            best_row = min_price_row

        best_by_ean[ean] = {
            "ean": best_row["ean"],
            "quantita": best_row["quantita"],
            "prezzo_iva_esclusa": best_row["prezzo_iva_esclusa"],
            "tag": best_row["tag"],
            "sku": best_row["sku"]  # solo riferimento
        }

    print(f"\n📦 Prodotti finali: {len(best_by_ean)}")

    # -----------------------------
    # Scrittura CSV finale
    # -----------------------------
    fields = ["ean","quantita","prezzo_iva_esclusa","tag","sku"]
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out, fieldnames=fields, delimiter="|",
            quoting=csv.QUOTE_NONE, escapechar="\\"
        )
        writer.writeheader()
        for r in best_by_ean.values():
            writer.writerow(r)

    print(f"\n📝 Feed generato correttamente: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
