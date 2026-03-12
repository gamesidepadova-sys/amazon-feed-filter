import csv
import requests
import io
import re
from collections import defaultdict

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
# Utility
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
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except Exception:
        return default

def to_float(x, default=0.0) -> float:
    try:
        s = str(x or "").strip()
        if not s: return default
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return default

def supplier_from_sku(sku: str) -> str:
    sku = sku or ""
    m = re.search(r"(03[0-9]{2})", sku)
    return m.group(1) if m else ""

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

def valid_ean(ean: str) -> bool:
    if not ean:
        return False
    e = ean.strip()
    return e.isdigit() and 8 <= len(e) <= 14

# -----------------------------
# Main
# -----------------------------
def main():
    print("📥 Scarico feed originale...")
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")

    delim = detect_delim(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso","tag"
    ]

    ean_dict = defaultdict(list)

    # -----------------------------
    # Lettura feed originale
    # -----------------------------
    for r in reader:
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

            ean = clean_text(r.get("ean") or "")
            if not valid_ean(ean):
                continue

            prezzo_raw = r.get("prezzo_iva_esclusa") or ""
            prezzo_num = to_float(prezzo_raw)

            row = {}
            for k in fields:
                if k == "quantita":
                    row[k] = qty
                elif k == "prezzo_iva_esclusa":
                    row[k] = clean_text(prezzo_raw)
                else:
                    row[k] = clean_text(r.get(k) or "")

            # Variabili interne
            row["_supplier"] = supplier
            row["_price_num"] = prezzo_num
            row["_sku_orig"] = sku
            ean_dict[ean].append(row)

        except Exception:
            continue

    rows_out = []

    # -----------------------------
    # Aggiornamento logica SKU con duplicazione solo se serve
    # -----------------------------
    for ean, items in ean_dict.items():
        sku_storico = items[0]
        supplier_storico = sku_storico["_supplier"]

        # Trova il prezzo migliore
        best_item = min(items, key=lambda x: x["_price_num"])
        best_supplier = best_item["_supplier"]

        # 1️⃣ Aggiorna sempre lo SKU storico
        active_row = {k: v for k, v in sku_storico.items() if k not in ["_supplier","_price_num","_sku_orig"]}
        active_row["sku"] = sku_storico["_sku_orig"]
        # Tag solo per categoria/marca
        active_row["tag"] = f"{active_row.get('cat1','')},{active_row.get('marca','')}"
        rows_out.append(active_row)

        # 2️⃣ Se il prezzo migliore è di un altro fornitore, creiamo un nuovo SKU
        if best_supplier != supplier_storico:
            new_row = {k: v for k, v in best_item.items() if k not in ["_supplier","_price_num","_sku_orig"]}
            new_row["sku"] = f"{best_supplier}_{best_item['_sku_orig']}"
            new_row["tag"] = f"{new_row.get('cat1','')},{new_row.get('marca','')}"
            rows_out.append(new_row)

    # -----------------------------
    # Scrittura CSV finale
    # -----------------------------
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out, fieldnames=fields, delimiter="|",
            quoting=csv.QUOTE_NONE, escapechar="\\"
        )
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)

    print(f"\n📝 Feed generato correttamente: {OUTPUT_FILE}")
    print(f"📊 Righe finali: {len(rows_out)}")

if __name__ == "__main__":
    main()
