import re

# ----------------------
# Configurazione
# ----------------------
INPUT_FILE = "original.csv"            # il file sorgente
OUTPUT_FILE = "filtered_excel_ready.csv"  # file filtrato pronto per Excel

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0383"}
ALLOWED_CAT1 = {"informatica", "audio e tv", "clima e brico", "consumabili e ufficio", "salute, beauty e fitness"}
MIN_QTY = 10
EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory", "montatura"}  # case insensitive

# ----------------------
# Funzioni di supporto
# ----------------------
def supplier_from_sku(sku):
    parts = (sku or "").split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    for p in parts:
        if len(p) == 4 and p.isdigit(): 
            return p
    return ""

def norm(s):
    return str(s or "").strip().lower()

def to_int(x, default=0):
    try:
        return int(float(str(x or "").replace(",", ".")))
    except:
        return default

# ----------------------
# Script principale
# ----------------------
rows_in = 0
rows_out = 0
skipped_supplier = 0
skipped_cat = 0
skipped_qty = 0
skipped_title = 0

with open(INPUT_FILE, "r", encoding="utf-8-sig") as fin, open(OUTPUT_FILE, "w", encoding="utf-8-sig") as fout:
    header = fin.readline()
    fout.write(header)  # scrive intestazione originale

    for line in fin:
        rows_in += 1
        cols = line.strip("\n").split("|")
        if len(cols) < 5:
            continue  # riga corrotta, saltata

        cat1 = norm(cols[0])
        sku = cols[1].strip()
        title = norm(cols[6])
        qty = to_int(cols[4])

        supplier = supplier_from_sku(sku)

        if supplier not in ALLOWED_SUPPLIERS:
            skipped_supplier += 1
            continue
        if cat1 not in ALLOWED_CAT1:
            skipped_cat += 1
            continue
        if qty < MIN_QTY:
            skipped_qty += 1
            continue
        if any(substr in title for substr in EXCLUDE_TITLE_SUBSTRINGS):
            skipped_title += 1
            continue

        # Scrive **esattamente la riga originale** senza modifiche
        fout.write(line)
        rows_out += 1

# ----------------------
# Report finale
# ----------------------
print(f"CSV filtrato pronto! Rows in: {rows_in}, Rows out: {rows_out}")
print(f"Skipped supplier: {skipped_supplier}, cat: {skipped_cat}, qty: {skipped_qty}, title: {skipped_title}")
print(f"Output file: {OUTPUT_FILE}")
