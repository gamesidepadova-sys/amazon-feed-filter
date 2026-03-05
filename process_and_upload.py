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
        if not s:
            return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default


def supplier_from_sku(sku: str) -> str:
    parts = (sku or "").split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""


def norm(s: str) -> str:
    return str(s or "").strip().lower()


def clean_text(s: str) -> str:
    s = str(s or "")
    s = re.sub(r"[\x00-\x1F]", "", s)  # invisibili
    s = s.replace('""', "'")
    s = re.sub(r"<[^>]+>", "", s)  # HTML
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return s.strip()


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

    ORIGINAL_COLUMNS = reader.fieldnames[:]  # tutte le colonne presenti nel file
    print(">>> HEADER ORIGINALE:", ORIGINAL_COLUMNS)

    rows_in = 0
    rows_out = 0

    # Scrittura UTF‑8 con BOM
    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=ORIGINAL_COLUMNS,
            delimiter="|",
            lineterminator="\n",
            quoting=csv.QUOTE_ALL
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

            # Ricostruzione riga con tutte le colonne originali
            cleaned_row = {col: clean_text(row.get(col, "")) for col in ORIGINAL_COLUMNS}

            # Forza la presenza di tutte le colonne
            for col in ORIGINAL_COLUMNS:
                if cleaned_row[col] is None:
                    cleaned_row[col] = ""

            writer.writerow(cleaned_row)
            rows_out += 1

    print(f"CSV filtrato pronto!")
    print(f"Rows in: {rows_in}, Rows out: {rows_out}, Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
