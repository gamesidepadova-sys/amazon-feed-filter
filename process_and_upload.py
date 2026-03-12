import csv
import requests
import io
import re
from collections import defaultdict

INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "feed_poleepo.csv"

ALLOWED_SUPPLIERS = {"0372","0373","0374","0380","0381","0382","0383"}

ALLOWED_CAT1 = {
    "informatica",
    "audio e tv",
    "clima e brico",
    "consumabili e ufficio",
    "salute, beauty e fitness",
}

EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory","montatura"}

MIN_QTY = 10


# -------------------------
# Utility
# -------------------------

def detect_delim(text):
    try:
        return csv.Sniffer().sniff(text[:5000], delimiters=",;\t|").delimiter
    except:
        return ","


def to_int(x):
    try:
        s = str(x).strip().replace(",",".")
        return int(float(s))
    except:
        return 0


def to_float(x):
    try:
        s = str(x).strip().replace(",",".")
        return float(s)
    except:
        return 0.0


def supplier_from_sku(sku):
    m = re.search(r"(03[0-9]{2})", sku or "")
    return m.group(1) if m else ""


def norm(x):
    return str(x or "").strip().lower()


def clean_text(t):
    t = str(t or "")
    t = re.sub("<.*?>"," ",t)
    t = t.replace("&nbsp;"," ")
    t = t.replace("|"," ")
    t = t.replace('"',"")
    t = re.sub(r"\s+"," ",t)
    return t.strip()


def valid_ean(ean):
    e = str(ean or "").strip()
    return e.isdigit() and 8 <= len(e) <= 14


# -------------------------
# Main
# -------------------------

def main():

    print("📥 Download feed...")

    resp = requests.get(INPUT_URL)
    resp.raise_for_status()

    text = resp.content.decode("utf-8-sig","replace")

    delim = detect_delim(text)

    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    fields = [
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso"
    ]

    ean_map = defaultdict(list)

    rows_read = 0
    rows_valid = 0

    for r in reader:

        rows_read += 1

        sku = r.get("sku") or r.get("SKU") or ""
        supplier = supplier_from_sku(sku)

        if supplier not in ALLOWED_SUPPLIERS:
            continue

        cat1 = norm(r.get("cat1") or r.get("categoria"))

        if cat1 not in ALLOWED_CAT1:
            continue

        titolo = norm(r.get("titolo_prodotto") or r.get("nome"))

        if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
            continue

        qty = to_int(r.get("quantita") or r.get("qty"))

        if qty < MIN_QTY:
            continue

        ean = clean_text(r.get("ean"))

        if not valid_ean(ean):
            continue

        price_raw = r.get("prezzo_iva_esclusa")
        price_num = to_float(price_raw)

        if price_num <= 0:
            continue

        row = {
            "cat1": clean_text(r.get("cat1")),
            "sku": clean_text(sku),
            "ean": ean,
            "mpn": clean_text(r.get("mpn")),
            "quantita": qty,
            "prezzo_iva_esclusa": f"{price_num:.2f}",
            "titolo_prodotto": clean_text(r.get("titolo_prodotto")),
            "immagine_principale": clean_text(r.get("immagine_principale")),
            "descrizione_prodotto": clean_text(r.get("descrizione_prodotto")),
            "costo_spedizione": clean_text(r.get("costo_spedizione")),
            "cat2": clean_text(r.get("cat2")),
            "cat3": clean_text(r.get("cat3")),
            "marca": clean_text(r.get("marca")),
            "peso": clean_text(r.get("peso")),
            "_supplier": supplier,
            "_price": price_num
        }

        ean_map[ean].append(row)

        rows_valid += 1


    print("Righe lette:", rows_read)
    print("Righe valide:", rows_valid)
    print("EAN unici:", len(ean_map))


    rows_out = []

    for ean, items in ean_map.items():

        preferred = [x for x in items if x["_supplier"] == "0373"]

        if preferred:
            chosen = min(preferred, key=lambda x: x["_price"])
        else:
            chosen = min(items, key=lambda x: x["_price"])

        final_row = {k:v for k,v in chosen.items() if not k.startswith("_")}

        rows_out.append(final_row)


    print("Prodotti finali:", len(rows_out))


    with open(OUTPUT_FILE,"w",encoding="utf-8",newline="") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            escapechar="\\"
        )

        writer.writeheader()

        for r in rows_out:
            writer.writerow(r)


    print("✅ Feed generato:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
