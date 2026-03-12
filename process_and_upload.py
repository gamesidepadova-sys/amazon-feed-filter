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

def to_float(x, default=0.0) -> float:
    try:
        s = str(x or "").strip()
        if not s: return default
        return float(s.replace(",", "."))
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
    t = re.sub("<.*?>", " ", t)  # rimuove HTML
    t = t.replace("&nbsp;", " ")
    t = t.replace('"', "")
    t = t.replace("|", " ")
    t = t.replace("\n", " ")
    t = t.replace("\r", " ")
    t = re.sub(" +", " ", t)
    return t.strip()

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

    # Raggruppa righe per EAN
    ean_dict = defaultdict(list)
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

            prezzo = to_float(r.get("prezzo_iva_esclusa"))
            row = {k: clean_text(r.get(k)) if k not in ["quantita","prezzo_iva_esclusa"] else (qty if k=="quantita" else prezzo) for k in fields}
            row["_supplier"] = supplier  # per regola 0373
            ean_dict[row["ean"]].append(row)
        except Exception as e:
            error_rows.append((i, str(e)))

    if not ean_dict:
        raise Exception("❌ Feed vuoto dopo i filtri: upload bloccato!")

    print(f"✅ Prodotti filtrati per EAN: {len(ean_dict)}")
    if error_rows:
        print(f"⚠ Righe con errore: {len(error_rows)} (es. riga {error_rows[0][0]})")

    # Seleziona il prezzo migliore per EAN con regola 0373
    rows_out = []
    for ean, items in ean_dict.items():
        chosen = None
        # Trova tutti con supplier 0373
        preferred = [x for x in items if x["_supplier"] == "0373"]
        if preferred:
            # Prendi quello con prezzo più basso tra 0373
            chosen = min(preferred, key=lambda x: x["prezzo_iva_esclusa"])
        else:
            # Prendi prezzo minimo tra tutti
            chosen = min(items, key=lambda x: x["prezzo_iva_esclusa"])
        rows_out.append({k:v for k,v in chosen.items() if k!="_supplier"})

    # Scrittura feed finale
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
