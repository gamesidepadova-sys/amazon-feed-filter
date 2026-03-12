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
PREF_0373_DELTA = 20  # € massimo in più per preferire 0373

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
        s = str(x or "").strip().replace(",", ".")
        return float(s) if s else default
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

    ean_dict = {}  # deduplica per EAN con preferenza 0373

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

            ean = row["ean"]
            if ean in ean_dict:
                # confronto con regola 0373
                existing = ean_dict[ean]
                # preferenza 0373 se entro delta
                if supplier == "0373" and (prezzo <= existing["prezzo_iva_esclusa"] + PREF_0373_DELTA):
                    ean_dict[ean] = row
                # altrimenti scegli prezzo più basso
                elif prezzo < existing["prezzo_iva_esclusa"]:
                    ean_dict[ean] = row
            else:
                ean_dict[ean] = row
        except Exception as e:
            print(f"⚠ Riga {i} errore: {e}")

    rows_out = list(ean_dict.values())

    if not rows_out:
        raise Exception("❌ Feed vuoto dopo i filtri: upload bloccato!")

    print(f"✅ Prodotti filtrati e deduplicati: {len(rows_out)}")

    # Scrittura feed finale
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out, fieldnames=fields, delimiter="|",
            quoting=csv.QUOTE_NONE, escapechar="\\"
        )
        writer.writeheader()
        for r in rows_out:
            # scrittura sicura: quantità come int, prezzo come float con 2 decimali
            r_out = r.copy()
            r_out["quantita"] = int(r_out["quantita"])
            r_out["prezzo_iva_esclusa"] = f"{float(r_out['prezzo_iva_esclusa']):.2f}"
            writer.writerow(r_out)

    print(f"📝 Feed generato correttamente: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
