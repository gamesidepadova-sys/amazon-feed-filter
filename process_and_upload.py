import csv
import re

# ----------------------
# Configurazione
# ----------------------
INPUT_FILE = "filtered_clean.csv"          # il file già presente in Drive
OUTPUT_FILE = "filtered_clean_fixed.csv"   # file finale corretto per Excel

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
        if not s:
            return default
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

def clean_text(s: str) -> str:
    """Rimuove caratteri invisibili, HTML e doppi apici"""
    s = str(s or "")
    s = re.sub(r"[\x00-\x1F]", "", s)
    s = s.replace('""', "'")
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("\r\n", "\n")
    s = s.replace("\r", "\n")
    return s

# ----------------------
# Script principale
# ----------------------
def main():
    # Determina il delimitatore leggendo la prima riga
    with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
        first_line = f.readline()
        if "|" in first_line:
            delim = "|"
        elif ";" in first_line:
            delim = ";"
        elif "\t" in first_line:
            delim = "\t"
        else:
            delim = ","

    rows_in = 0
    rows_out = 0

    with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as fin, \
         open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError("Il CSV di input non ha header")

        writer = csv.DictWriter(
            fout,
            fieldnames=reader.fieldnames,
            delimiter="|",      # mantenuto come nel file originale
            lineterminator="\n",
            quoting=csv.QUOTE_ALL,  # tutte le celle tra virgolette
        )
        writer.writeheader()

        for row in reader:
            rows_in += 1
            sku = (row.get("sku") or "").strip()
            if not sku:
                continue

            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = norm(row.get("cat1"))
            if cat1 not in ALLOWED_CAT1:
                continue

            qty = to_int(row.get("quantita"))
            if qty < MIN_QTY:
                continue

            title = norm(row.get("titolo_prodotto"))
            if any(substr in title for substr in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            # pulizia dei campi
            cleaned_row = {k: clean_text(v) for k, v in row.items()}
            writer.writerow(cleaned_row)
            rows_out += 1

    print(f"CSV pronto! Rows in: {rows_in}, Rows out: {rows_out}, Output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
