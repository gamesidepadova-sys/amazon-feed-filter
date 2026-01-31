import csv

SOURCE_FILE = "source.csv"
OUTPUT_FILE = "filtered.csv"

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}
MIN_QTY = 10  # <-- filtro quantità: >= 10

# TODO: sostituisci con le tue categorie reali (stringhe esatte del CSV)
ALLOWED_CATEGORIES = {
    "CAT_A",
    "CAT_B",
    "CAT_C",
}

with open(SOURCE_FILE, newline="", encoding="utf-8") as fin, \
     open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as fout:

    reader = csv.DictReader(fin, delimiter="|")
    writer = csv.DictWriter(fout, fieldnames=reader.fieldnames, delimiter="|")
    writer.writeheader()

    for row in reader:
        # quantità
        try:
            qty = int((row.get("quantita") or "0").strip())
        except ValueError:
            qty = 0

        supplier = (row.get("supplier_code") or "").strip()
        category = (row.get("categoria") or "").strip()

        if qty < MIN_QTY:
            continue
        if supplier not in ALLOWED_SUPPLIERS:
            continue
        if category not in ALLOWED_CATEGORIES:
            continue

        writer.writerow(row)
