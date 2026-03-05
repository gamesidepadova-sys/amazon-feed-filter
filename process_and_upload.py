# csv_prepare_full_original.py
import csv
import re

INPUT_FILE = "original.csv"
OUTPUT_FILE = "filtered_clean.csv"

# ---------------- FILTRI ORIGINALI ----------------
ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}
ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "clima e brico",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}

MIN_QUANTITY = 10  # <-- quantità minima come era prima

# ---------------- FUNZIONI DI PULIZIA ----------------
def clean_text(text: str) -> str:
    if not text:
        return ""
    # Rimuove caratteri invisibili
    text = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", text)
    # Sostituisce doppie virgolette con singole
    text = text.replace('""', "'")
    # Rimuove tag HTML
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


# ---------------- LETTURA CSV ----------------
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    lines = f.readlines()

reader = csv.DictReader(lines, delimiter='|')
filtered_rows = []

for row in reader:
    # Pulizia dei valori
    cleaned_row = {k: clean_text(v) for k, v in row.items()}

    # ---------------- FILTRI ----------------
    # 1. Fornitore consentito (dai primi 4 caratteri di SKU)
    supplier = cleaned_row.get("sku", "")[:4]
    if supplier not in ALLOWED_SUPPLIERS:
        continue

    # 2. Categoria principale consentita
    if cleaned_row.get("cat1", "").lower() not in ALLOWED_CAT1:
        continue

    # 3. Quantità minima
    try:
        if int(cleaned_row.get("quantita", 0)) < MIN_QUANTITY:
            continue
    except ValueError:
        continue

    filtered_rows.append(cleaned_row)

# ---------------- SCRITTURA CSV ----------------
with open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=reader.fieldnames,
        delimiter='|',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )
    writer.writeheader()
    for r in filtered_rows:
        writer.writerow(r)

print(f"CSV filtrato e pulito pronto: {OUTPUT_FILE}")
