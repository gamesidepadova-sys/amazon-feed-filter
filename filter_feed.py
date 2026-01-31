import csv
import re

SOURCE_FILE = "source.csv"
OUTPUT_FILE = "filtered.csv"

MIN_QTY = 10  # quantità >= 10
ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}

# Inserisci qui le categorie ESATTE come appaiono in cat1
ALLOWED_CATEGORIES = {
    "Auto, Moto E Nautica",
    # "Altra Categoria",
}

SUPPLIER_RE = re.compile(r"^[A-Za-z]+_(\d{4})_")

def detect_delimiter(header_line: str) -> str:
    # Priorità: tab se presente, altrimenti pipe, altrimenti virgola
    if "\t" in header_line:
        return "\t"
    if "|" in header_line:
        return "|"
    return ","

def parse_int(x, default=0):
    try:
        return int(str(x or "").strip())
    except Exception:
        return default

def extract_supplier(sku: str) -> str:
    s = str(sku or "").strip()
    m = SUPPLIER_RE.match(s)
    return m.group(1) if m else ""

with open(SOURCE_FILE, "r", encoding="utf-8", newline="") as fin:
    first_line = fin.readline()
    if not first_line:
        raise RuntimeError("Empty source file")

    delimiter = detect_delimiter(first_line)

    # Riavvolgi e usa DictReader
    fin.seek(0)
    reader = csv.DictReader(fin, delimiter=delimiter)

    # Verifiche colonne
    needed = {"sku", "cat1"}
    missing = needed - set(reader.fieldnames or [])
    if missing:
        raise RuntimeError(f"Missing columns: {sorted(missing)}. Found: {reader.fieldnames}")

    # quantita può chiamarsi "quantita" (come nel tuo Apps Script) o altro: lo gestiamo flessibile
    qty_col_candidates = ["quantita", "qty", "quantity"]
    qty_col = next((c for c in qty_col_candidates if c in (reader.fieldnames or [])), None)
    if qty_col is None:
        # Se non esiste, non possiamo filtrare su qty: meglio fermarsi, così non fai un feed sbagliato.
        raise RuntimeError(f"Missing quantity column. Expected one of: {qty_col_candidates}. Found: {reader.fieldnames}")

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames, delimiter=delimiter)
        writer.writeheader()

        kept = 0
        seen = 0

        for row in reader:
            seen += 1

            category = (row.get("cat1") or "").strip()
            if category not in ALLOWED_CATEGORIES:
                continue

            sku_raw = row.get("sku") or ""
            supplier = extract_supplier(sku_raw)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            qty = parse_int(row.get(qty_col), 0)
            if qty < MIN_QTY:
                continue

            writer.writerow(row)
            kept += 1

        print(f"Delimiter={repr(delimiter)} qty_col={qty_col} rows_in={seen} rows_out={kept}")
