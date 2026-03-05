import csv
import html
import re
import requests
from collections import defaultdict

# ----------------------
# Configurazione
# ----------------------
INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "filtered_clean.csv"

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}
ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "clima e brico",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}
EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory", "montatura"}  # case insensitive
MIN_QTY = 10

# ----------------------
# Funzioni di supporto
# ----------------------
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
        return int(float(s.replace(",", ".")))
    except Exception:
        return default

def to_float(x, default=0.0) -> float:
    try:
        s = str(x or "").strip()
        if not s: return default
        return float(s.replace(",", "."))
    except Exception:
        return default

def supplier_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    for p in parts:
        if len(p) == 4 and p.isdigit(): return p
    return ""

def norm(s: str) -> str:
    return str(s or "").strip().lower()

def clean_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text)
    text = text.replace('""', "'")
    return text.strip()

# ----------------------
# Script principale
# ----------------------
def main():
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")
    delim = detect_delim(text)

    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    if not reader.fieldnames:
        raise RuntimeError("Il CSV scaricato non ha header")

    required = {"cat1", "sku", "quantita", "prezzo_iva_esclusa", "titolo_prodotto", "descrizione_prodotto"}
    missing = [c for c in required if c not in set(reader.fieldnames)]
    if missing:
        raise RuntimeError(f"Colonne mancanti: {missing}. Header={reader.fieldnames}")

    rows_in = 0
    rows_out = 0

    # dict per gestire SKU duplicati: mantiene solo la quantità maggiore
    sku_map = defaultdict(dict)

    for row in reader:
        rows_in += 1
        sku = (row.get("sku") or "").strip()
        if not sku: continue

        supplier = supplier_from_sku(sku)
        if supplier not in ALLOWED_SUPPLIERS: continue

        cat1 = norm(row.get("cat1"))
        if cat1 not in ALLOWED_CAT1: continue

        qty = to_int(row.get("quantita"))
        if qty < MIN_QTY: continue

        title = norm(row.get("titolo_prodotto"))
        if any(substr in title for substr in EXCLUDE_TITLE_SUBSTRINGS): continue

        price = to_float(row.get("prezzo_iva_esclusa"), default=0.0)
        if price <= 0.0: continue  # elimina prodotti senza prezzo

        # Pulizia dei campi
        cleaned_row = {k: clean_text(v) for k, v in row.items()}

        # gestisci duplicati SKU: conserva la riga con quantità maggiore
        if sku in sku_map:
            if cleaned_row.get("quantita", 0) > sku_map[sku].get("quantita", 0):
                sku_map[sku] = cleaned_row
        else:
            sku_map[sku] = cleaned_row

    # scrive CSV finale
    fieldnames = reader.fieldnames
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames, delimiter="|", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for row in sku_map.values():
            writer.writerow(row)
            rows_out += 1

    print(f"CSV filtrato pronto! Rows in: {rows_in}, Rows out: {rows_out}, Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
