import csv
import requests

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
MIN_QTY = 10

# ----------------------
# Funzioni di supporto
# ----------------------
def supplier_from_sku(sku):
    parts = (sku or "").split("_")
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""

def norm(s):
    return str(s or "").strip().lower()

def to_int(x):
    try:
        return int(float(str(x or "").replace(",", ".")))
    except:
        return 0

# ----------------------
# Script principale
# ----------------------
def main():
    # Scarica CSV originale
    resp = requests.get(INPUT_URL, stream=True)
    resp.raise_for_status()
    lines = (line.decode('utf-8-sig') for line in resp.iter_lines())
    reader = csv.DictReader(lines, delimiter='|')
    fieldnames = reader.fieldnames

    if not fieldnames:
        raise RuntimeError("CSV senza header")

    rows_in = 0
    rows_out = 0

    # Scrive CSV filtrato senza modificare contenuto
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

            # Scrive la riga così com’è
            writer.writerow(row)
            rows_out += 1

    print(f"CSV filtrato pronto: {OUTPUT_FILE}")
    print(f"Righe lette: {rows_in}, Righe scritte: {rows_out}")

if __name__ == "__main__":
    main()
