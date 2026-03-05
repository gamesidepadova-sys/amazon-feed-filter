import csv
import requests
import re

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
EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory", "montatura"}
MIN_QTY = 10

# ----------------------
# Funzioni di supporto
# ----------------------
def to_int(x, default=0):
    try:
        return int(float(str(x or "").replace(",", ".")))
    except:
        return default

def supplier_from_sku(sku):
    parts = (sku or "").split("_")
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""

def norm(s): return str(s or "").strip().lower()

def clean_text(s):
    """Pulisce testo da invisibili e line break senza alterare virgolette"""
    s = str(s or "")
    s = re.sub(r"[\x00-\x1F]", "", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return s.strip()

# ----------------------
# Script principale
# ----------------------
def main():
    with requests.get(INPUT_URL, stream=True) as r:
        r.raise_for_status()
        lines = (line.decode('utf-8-sig') for line in r.iter_lines())
        reader = csv.DictReader(lines, delimiter='|')
        fieldnames = reader.fieldnames

        if not fieldnames:
            raise RuntimeError("Il CSV scaricato non ha header")

        rows_in = 0
        rows_out = 0

        # Scrittura con BOM, delimitatore |, virgolette, line endings Windows
        with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline='') as fout:
            writer = csv.DictWriter(
                fout,
                fieldnames=fieldnames,
                delimiter='|',
                quoting=csv.QUOTE_ALL,
                lineterminator="\r\n"
            )
            writer.writeheader()

            for row in reader:
                rows_in += 1
                sku = row.get("sku", "").strip()
                if not sku or supplier_from_sku(sku) not in ALLOWED_SUPPLIERS:
                    continue
                if norm(row.get("cat1")) not in ALLOWED_CAT1:
                    continue
                if to_int(row.get("quantita")) < MIN_QTY:
                    continue
                title = norm(row.get("titolo_prodotto"))
                if any(substr in title for substr in EXCLUDE_TITLE_SUBSTRINGS):
                    continue

                cleaned_row = {col: clean_text(row.get(col, "")) for col in fieldnames}
                writer.writerow(cleaned_row)
                rows_out += 1

    print(f"CSV filtrato pronto!")
    print(f"Rows in: {rows_in}, Rows out: {rows_out}, Output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
