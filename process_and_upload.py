import csv
import re
import os

# ----------------------
# Configurazione
# ----------------------
INPUT_FILE = "filtered_clean.csv"   # il file già presente in Drive / workflow
OUTPUT_FILE = "filtered_clean_fixed.csv"

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
def to_int(x, default=0) -> int:
    try:
        s = str(x or "").strip()
        if not s: return default
        return int(float(s.replace(",", ".")))
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

def clean_text(s: str) -> str:
    """Rimuove caratteri invisibili e HTML ma mantiene linebreak per Excel"""
    s = str(s or "")
    s = re.sub(r"[\x00-\x1F]", "", s)  # rimuove caratteri invisibili
    s = s.replace('""', "'")
    s = re.sub(r"<[^>]+>", "", s)  # rimuove tag HTML
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return s

# ----------------------
# Script principale
# ----------------------
def main():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"File non trovato: {INPUT_FILE}")

    # Rileva delimiter dal file originale
    with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
        sample = f.read(8192)
        try:
            delim = csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
        except Exception:
            delim = "|"

    with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as fin, \
         open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError("Il CSV non ha header")

        required = {"cat1", "sku", "quantita", "prezzo_iva_esclusa", "titolo_prodotto"}
        missing = [c for c in required if c not in set(reader.fieldnames)]
        if missing:
            raise RuntimeError(f"Colonne mancanti: {missing}. Header={reader.fieldnames}")

        writer = csv.DictWriter(
            fout,
            fieldnames=reader.fieldnames,
            delimiter="|",
            lineterminator="\n",
            quoting=csv.QUOTE_ALL  # <<< Tutte le celle tra virgolette
        )
        writer.writeheader()

        rows_in = 0
        rows_out = 0

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

            # Pulizia dei campi
            cleaned_row = {k: clean_text(v) for k, v in row.items()}
            writer.writerow(cleaned_row)
            rows_out += 1

    print(f"CSV filtrato pronto! Rows in: {rows_in}, Rows out: {rows_out}, Output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
