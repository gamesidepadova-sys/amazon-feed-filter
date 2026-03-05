import csv
import requests
import io

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

TARGET_HEADERS = [
    "cat1",
    "sku",
    "ean",
    "mpn",
    "quantita",
    "prezzo_iva_esclusa",
    "titolo_prodotto",
    "immagine_principale",
    "descrizione_prodotto",
    "costo_spedizione"
]

def supplier_from_sku(sku):
    parts = sku.split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""

def to_int(x):
    try:
        return int(float(str(x).replace(",", ".")))
    except:
        return 0

def main():
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")

    f = io.StringIO(text)
    reader = csv.DictReader(f, delimiter="|")

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        out.write("|".join(TARGET_HEADERS) + "\n")

        for row in reader:
            sku = (row.get("sku") or "").strip()
            if not sku:
                continue

            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = (row.get("cat1") or "").strip().lower()
            if cat1 not in ALLOWED_CAT1:
                continue

            qty = to_int(row.get("quantita"))
            if qty < MIN_QTY:
                continue

            title = (row.get("titolo_prodotto") or "").lower()
            if any(bad in title for bad in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            values = [row.get(k, "") for k in TARGET_HEADERS]
            out.write("|".join(values) + "\n")

    print("Creato:", OUTPUT_FILE)

if __name__ == "__main__":
    main()
