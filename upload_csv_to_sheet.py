import csv
from pathlib import Path

# Input/Output
INPUT_FILE = "source.csv"
OUTPUT_FILE = "filtered.csv"

# Filtri
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

# -------------------------
# Funzioni di supporto
# -------------------------
def detect_delim(text: str) -> str:
    """Auto-detect CSV delimiter: tab -> pipe -> semicolon -> comma."""
    try:
        sample = text[:8192]
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return d.delimiter
    except Exception:
        first = text.splitlines()[0] if text else ""
        if "\t" in first:
            return "\t"
        if "|" in first:
            return "|"
        if ";" in first and first.count(";") > first.count(","):
            return ";"
        return ","

def to_int(x, default=0) -> int:
    """Convert string/float to int safely."""
    try:
        s = str(x or "").strip()
        if not s:
            return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default

def supplier_from_sku(sku: str) -> str:
    """Estrae il codice fornitore dal formato SKU tipico: T_0372_17077617000"""
    parts = (sku or "").split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""

def norm(s: str) -> str:
    return str(s or "").strip().lower()

# -------------------------
# Script principale
# -------------------------
def main():
    # Leggi file sorgente
    raw = Path(INPUT_FILE).read_bytes()
    text = raw.decode("utf-8-sig", errors="replace")
    delim = detect_delim(text)

    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    if not reader.fieldnames:
        raise RuntimeError(f"{INPUT_FILE} has no header row")

    # Controllo colonne minime
    required = {"cat1", "sku", "quantita", "prezzo_iva_esclusa", "titolo_prodotto"}
    missing = [c for c in required if c not in set(reader.fieldnames)]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}. Header={reader.fieldnames}")

    rows_in = 0
    rows_out = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=reader.fieldnames,
            delimiter=delim,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()

        for row in reader:
            rows_in += 1

            sku = (row.get("sku") or "").strip()
            if not sku:
                continue

            # Supplier whitelist
            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            # Categoria
            cat1 = norm(row.get("cat1"))
            if cat1 not in ALLOWED_CAT1:
                continue

            # Quantità
            qty = to_int(row.get("quantita"), 0)
            if qty < MIN_QTY:
                continue

            # Esclusione titoli
            title = norm(row.get("titolo_prodotto"))
            if any(substr in title for substr in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            writer.writerow(row)
            rows_out += 1

    print("Filtered CSV ready!")
    print("Detected delimiter:", repr(delim))
    print(f"Rows read: {rows_in}")
    print(f"Rows written: {rows_out}")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
