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
        if not s:
            return default
        s = s.replace(".", "").replace(",", ".")
        return int(float(s))
    except Exception:
        return default

# 🔥 FIX PREZZO: NON rimuovere i punti
def to_float(x, default=0.0) -> float:
    try:
        s = str(x or "").strip()
        if not s:
            return default
        s = s.replace(",", ".")  # <-- FIX
        return float(s)
    except Exception:
        return default

# Regex robusta per trovare 0372–0383 ovunque nello SKU
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
        "costo_spedizione","cat2","cat3","marca","peso"
    ]

    ean_dict = defaultdict(list)
    error_rows = []

    stats = {
        "bad_supplier": [],
        "bad_cat1": [],
        "bad_title": [],
        "bad_qty": [],
        "bad_ean": [],
    }

    for i, r in enumerate(reader, 1):
        try:
            sku = r.get("sku") or r.get("SKU") or ""
            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                if len(stats["bad_supplier"]) < 3:
                    stats["bad_supplier"].append((i, sku))
                continue

            cat1 = norm(r.get("cat1") or r.get("categoria") or "")
            if cat1 not in ALLOWED_CAT1:
                if len(stats["bad_cat1"]) < 3:
                    stats["bad_cat1"].append((i, cat1))
                continue

            titolo = norm(r.get("titolo_prodotto") or r.get("nome") or "")
            if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
                if len(stats["bad_title"]) < 3:
                    stats["bad_title"].append((i, titolo))
                continue

            qty = to_int(r.get("quantita") or r.get("qty"))
            if qty < MIN_QTY:
                if len(stats["bad_qty"]) < 3:
                    stats["bad_qty"].append((i, qty))
                continue

            ean = clean_text(r.get("ean") or "")
            if not valid_ean(ean):
                if len(stats["bad_ean"]) < 3:
                    stats["bad_ean"].append((i, ean))
                continue

            prezzo = to_float(r.get("prezzo_iva_esclusa"))

            row = {}
            for k in fields:
                if k == "quantita":
                    row[k] = qty
                elif k == "prezzo_iva_esclusa":
                    row[k] = f"{prezzo:.2f}"
                else:
                    row[k] = clean_text(r.get(k) or "")

            row["_supplier"] = supplier
            ean_dict[ean].append(row)

        except Exception as e:
            error_rows.append((i, str(e)))

    print("\n📊 STATISTICHE FILTRI:")
    for key, items in stats.items():
        print(f" - {key}: {len(items)}")
        for ex in items:
            print(f"    es: riga {ex[0]} → {ex[1]}")

    if error_rows:
        print(f"\n⚠ Errori parsing: {len(error_rows)}")
        print(f"   es: riga {error_rows[0][0]} → {error_rows[0][1]}")

    if not ean_dict:
        raise Exception("❌ Feed vuoto dopo i filtri: upload bloccato!")

    print(f"\n📦 Prodotti raggruppati per EAN: {len(ean_dict)}")

    rows_out = []
    for ean, items in ean_dict.items():
        preferred = [x for x in items if x["_supplier"] == "0373"]
        if preferred:
            chosen = min(preferred, key=lambda x: float(x["prezzo_iva_esclusa"]))
        else:
            chosen = min(items, key=lambda x: float(x["prezzo_iva_esclusa"]))

        chosen = {k: v for k, v in chosen.items() if k != "_supplier"}
        rows_out.append(chosen)

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
