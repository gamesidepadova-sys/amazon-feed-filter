import csv
import re
from typing import List

SOURCE_FILE = "source.csv"
OUTPUT_FILE = "filtered.csv"

# Filtri richiesti
MIN_QTY = 10  # quantita >= 10
ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}
ALLOWED_CATEGORIES = {"Informatica"}  # match esatto (trim), case-sensitive

# ESCLUSIONE titolo: contiene "phs-memory" (case-insensitive)
EXCLUDE_TITLE_SUBSTR = "phs-memory"
TITLE_COLUMN = "titolo_prodotto"  # colonna reale del feed

# SKU formato atteso: S_0373_ABC..., T_0380_5854285, ecc.
SUPPLIER_RE = re.compile(r"^[A-Za-z]+_(\d{4})_")

def detect_delimiter(header_line: str) -> str:
    """Auto-detect: tab -> pipe -> comma."""
    if "\t" in header_line:
        return "\t"
    if "|" in header_line:
        return "|"
    return ","

def normalize_fieldnames(fieldnames: List[str]) -> List[str]:
    """Rimuove BOM e spazi; mantiene i nomi originali salvo pulizia minima."""
    out = []
    for fn in fieldnames:
        s = (fn or "")
        s = s.lstrip("\ufeff")  # BOM UTF-8 (es: '\ufeffcat1')
        s = s.strip()
        out.append(s)
    return out

def parse_int(x, default=0) -> int:
    try:
        return int(str(x or "").strip())
    except Exception:
        return default

def extract_supplier(sku: str) -> str:
    s = str(sku or "").strip()
    m = SUPPLIER_RE.match(s)
    return m.group(1) if m else ""

def open_text(path: str):
    # utf-8-sig rimuove BOM dal testo in modo naturale; comunque normalizziamo header anche dopo
    return open(path, "r", encoding="utf-8-sig", newline="")

def require_columns(fieldnames: List[str], required: List[str]) -> None:
    missing = [c for c in required if c not in fieldnames]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}. Found: {fieldnames}")

def main():
    with open_text(SOURCE_FILE) as fin:
        first_line = fin.readline()
        if not first_line:
            raise RuntimeError("Empty source file")

        delimiter = detect_delimiter(first_line)
        fin.seek(0)

        reader = csv.DictReader(fin, delimiter=delimiter)
        if not reader.fieldnames:
            raise RuntimeError("CSV has no header row / fieldnames")

        # Normalizza header (BOM, spazi)
        reader.fieldnames = normalize_fieldnames(reader.fieldnames)

        # Colonne richieste nel tuo feed (titolo_prodotto obbligatorio per filtro PHS)
        require_columns(reader.fieldnames, ["sku", "cat1", "quantita", TITLE_COLUMN])

        # Scriviamo nello stesso delimitatore del file (TSV/pipe/csv a seconda dell'input)
        with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames, delimiter=delimiter)
            writer.writeheader()

            rows_in = 0
            rows_out = 0
            skipped_cat = 0
            skipped_supplier = 0
            skipped_qty = 0
            skipped_badsku = 0
            skipped_phs = 0

            for row in reader:
                rows_in += 1

                # Categoria
                cat = (row.get("cat1") or "").strip()
                if cat not in ALLOWED_CATEGORIES:
                    skipped_cat += 1
                    continue

                # Supplier da SKU
                sku_raw = row.get("sku") or ""
                supplier = extract_supplier(sku_raw)
                if not supplier:
                    skipped_badsku += 1
                    continue
                if supplier not in ALLOWED_SUPPLIERS:
                    skipped_supplier += 1
                    continue

                # Quantità
                qty = parse_int(row.get("quantita"), 0)
                if qty < MIN_QTY:
                    skipped_qty += 1
                    continue

                # Filtro titolo: escludi se contiene "phs-memory" (case-insensitive)
                title = (row.get(TITLE_COLUMN) or "").strip().lower()
                if EXCLUDE_TITLE_SUBSTR in title:
                    skipped_phs += 1
                    continue

                writer.writerow(row)
                rows_out += 1

            # Log finale (visibile nei log Action)
            print(f"delimiter={repr(delimiter)}")
            print(f"title_column={TITLE_COLUMN} exclude_substr={EXCLUDE_TITLE_SUBSTR}")
            print(f"rows_in={rows_in} rows_out={rows_out}")
            print(
                f"skipped_cat={skipped_cat} "
                f"skipped_supplier={skipped_supplier} "
                f"skipped_qty={skipped_qty} "
                f"skipped_badsku={skipped_badsku} "
                f"skipped_phs={skipped_phs}"
            )

            if rows_out == 0:
                print("WARNING: filtered output is empty. Check category string and input data.")

if __name__ == "__main__":
    main()
