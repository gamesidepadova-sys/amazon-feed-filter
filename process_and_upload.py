import csv
import requests
import io
import re

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"

ALLOWED_SUPPLIERS = {"0372", "0373", "0374", "0380", "0381", "0382", "0383"}

ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "clima e brico",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}

EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory", "montatura"}
MIN_QTY = 10

# -----------------------------
# Funzioni di utilità
# -----------------------------
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
        if not s: return default
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

def clean_text(text: str) -> str:
    t = str(text or "")
    t = re.sub("<.*?>", " ", t)
    t = t.replace("&nbsp;", " ")
    t = t.replace('"', "")
    t = t.replace("|", " ")
    t = t.replace("\n", " ")
    t = t.replace("\r", " ")
    t = re.sub(" +", " ", t)
    return t.strip()

# -----------------------------
# Funzione scelta prodotto
# -----------------------------
def choose_product(rows):
    if len(rows) == 1:
        return rows[0]

    # ordinamento per prezzo crescente
    rows_sorted = sorted(rows, key=lambda x: x["price"])
    lowest = rows_sorted[0]["price"]
    second_lowest = rows_sorted[1]["price"] if len(rows_sorted) > 1 else lowest

    # 1️⃣ Priorità 0382
    for r in rows:
        if r["supplier"] == "0382" and r["qty"] >= 16 and r["price"] == lowest:
            return r

    # 2️⃣ Priorità 0381
    for r in rows:
        if r["supplier"] == "0381" and r["qty"] >= 16 and r["price"] == lowest:
            return r

    # 3️⃣ Priorità 0373 entro +20€ rispetto al secondo prezzo più basso
    for r in rows:
        if r["supplier"] == "0373" and r["qty"] >= 16 and r["price"] <= second_lowest + 20:
            return r

    # 4️⃣ 0372 o 0380 → prezzo più basso tra quelli con stock >=16
    candidates = [r for r in rows if r["supplier"] in {"0372","0380"} and r["qty"] >= 16]
    if candidates:
        return sorted(candidates, key=lambda x: x["price"])[0]

    # 5️⃣ 0383 solo se unico
    if len(rows) == 1 and rows[0]["supplier"] == "0383":
        return rows[0]

    # fallback: prezzo più basso
    return rows_sorted[0]

# -----------------------------
# Main
# -----------------------------
def main():
    print("📥 Scarico feed originale...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")
    f = io.StringIO(text)
    delim = detect_delim(text)
    reader = csv.DictReader(f, delimiter=delim)

    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso"
    ]

    products_by_ean = {}
    error_rows = []

    for i, r in enumerate(reader, 1):
        try:
            sku = r.get("sku") or r.get("SKU") or ""
            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = norm(r.get("cat1") or r.get("categoria") or "")
            if cat1 not in ALLOWED_CAT1:
                continue

            titolo = norm(r.get("titolo_prodotto") or r.get("nome") or "")
            if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            qty = to_int(r.get("quantita") or r.get("qty"))
            if qty < MIN_QTY:
                continue

            prezzo = float(str(r.get("prezzo_iva_esclusa") or "0").replace(",", "."))
            ean = r.get("ean") or ""

            row = {
                "raw": r,
                "sku": sku,
                "supplier": supplier,
                "qty": qty,
                "price": prezzo
            }

            products_by_ean.setdefault(ean, []).append(row)

        except Exception as e:
            error_rows.append((i, str(e)))

    if not products_by_ean:
        raise Exception("❌ Feed vuoto dopo i filtri: upload bloccato!")

    rows_out = []
    for ean, rows in products_by_ean.items():
        chosen = choose_product(rows)
        if not chosen:
            continue  # sicurezza, non scrivere nulla se non scelto

        r = chosen["raw"]
        row = {k: clean_text(r.get(k)) if k != "quantita" else chosen["qty"] for k in fields}
        rows_out.append(row)

    print(f"✅ Prodotti finali: {len(rows_out)}")
    if error_rows:
        print(f"⚠ Righe con errore: {len(error_rows)} (es. riga {error_rows[0][0]})")

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out, fieldnames=fields, delimiter="|",
            quoting=csv.QUOTE_NONE, escapechar="\\"
        )
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)

    print(f"📝 Feed generato correttamente: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

# -----------------------------
# TEST LOGICA EAN
# -----------------------------
def test_choose_ean():
    rows = [
        {"raw": {}, "sku": "SKU_0373_XYZ", "supplier": "0373", "qty": 30, "price": 73.05},
        {"raw": {}, "sku": "SKU_0381_ABC", "supplier": "0381", "qty": 30, "price": 72.54},
        {"raw": {}, "sku": "SKU_0380_DEF", "supplier": "0380", "qty": 42, "price": 72.50},
    ]

    chosen = choose_product(rows)
    print("✅ SKU scelto per EAN 6933412728917:", chosen["sku"])
    print("Fornitore:", chosen["supplier"], "Prezzo:", chosen["price"], "Giacenza:", chosen["qty"])

if __name__ == "__main__":
    test_choose_ean()
