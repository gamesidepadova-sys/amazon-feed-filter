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
        if not s:
            return default
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except Exception:
        return default

def to_float(x, default=0.0) -> float:
    try:
        s = str(x or "").strip()
        if not s:
            return default
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

    best_by_ean = {}  # memorizza solo il migliore
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

            ean = clean_text(r.get("ean") or "")
            if not valid_ean(ean):
                continue

            prezzo_raw = r.get("prezzo_iva_esclusa") or ""
            prezzo_num = to_float(prezzo_raw)
            spedizione = to_float(r.get("costo_spedizione"))
            prezzo_totale = prezzo_num + spedizione  # prezzo reale

            marca = clean_text(r.get("marca") or "")

            row = {k: clean_text(r.get(k) or "") for k in fields}
            row["sku"] = ean
            row["quantita"] = qty
            row["prezzo_iva_esclusa"] = clean_text(prezzo_raw)
            row["tag"] = f"supplier_{supplier}"
            row["_supplier"] = supplier
            row["_price"] = prezzo_totale
            row["_marca"] = marca
            row["_titolo"] = titolo

            # Protezione bundle: se marca diversa non unire
            if ean not in best_by_ean:
                best_by_ean[ean] = row
            else:
                existing = best_by_ean[ean]
                if existing["_marca"] != marca:
                    continue  # ignora SKU diverso
                # scegli prezzo reale più basso
                if row["_price"] < existing["_price"]:
                    best_by_ean[ean] = row

        except Exception as e:
            error_rows.append((i, str(e)))

    if error_rows:
        print(f"⚠ Righe con errore: {len(error_rows)} (es. riga {error_rows[0][0]})")

    if not best_by_ean:
        raise Exception("❌ Feed vuoto dopo filtri!")

    print(f"\n📦 Prodotti finali: {len(best_by_ean)}")

    # -----------------------------
    # Scrittura CSV finale
    # -----------------------------
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out, fieldnames=fields, delimiter="|",
            quoting=csv.QUOTE_NONE, escapechar="\\"
        )
        writer.writeheader()
        for r in best_by_ean.values():
            # rimuovi chiavi interne
            for k in ["_supplier", "_price", "_marca", "_titolo"]:
                if k in r:
                    del r[k]
            writer.writerow(r)

    print(f"\n📝 Feed generato correttamente: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
