import csv
from pathlib import Path

INPUT_FILE = "source.csv"
OUTPUT_FILE = "filtered.csv"

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}

ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "clima e brico",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}

EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory", "montatura"}


def detect_delim(text: str) -> str:
    sample = text[:8192]
    try:
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return d.delimiter
    except Exception:
        first = text.splitlines()[0] if text else ""
        # fallback semplice
        if "\t" in first:
            return "\t"
        if "|" in first:
            return "|"
        if ";" in first and first.count(";") > first.count(","):
            return ";"
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
    # SKU tipico: T_0372_17077617000
    parts = (sku or "").split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    # fallback: cerca un blocco 4 cifre tra gli underscore
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""


def norm(s: str) -> str:
    return str(s or "").strip().lower()


def main():
    raw = Path(INPUT_FILE).read_bytes()
    text = raw.decode("utf-8-sig", errors="replace")
    delim = detect_delim(text)

    rows_in = 0
    rows_out = 0

    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    if not reader.fieldnames:
        raise RuntimeError(f"{INPUT_FILE} has no header")

    # Verifica colonne minime (con i tuoi header reali)
    required = {"cat1", "sku", "quantita", "prezzo_iva_esclusa", "titolo_prodotto"}
    missing = [c for c in required if c not in set(reader.fieldnames)]
    if missing:
        raise RuntimeError(f"Missing required columns in {INPUT_FILE}: {missing}. Header={reader.fieldnames}")

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

            # Supplier whitelist (da SKU)
            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            # Categoria whitelist su cat1
            cat1 = norm(row.get("cat1"))
            if cat1 not in ALLOWED_CAT1:
                continue

            # Quantità >= 10
            qty = to_int(row.get("quantita"), 0)
            if qty < 10:
                continue

            # Esclusioni sul titolo
            title = norm(row.get("titolo_prodotto"))
            if any(bad in title for bad in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            writer.writerow(row)
            rows_out += 1

    print("Detected delimiter:", repr(delim))
    print(f"Rows in: {rows_in}")
    print(f"Rows out: {rows_out}")
    print(f"Wrote: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
