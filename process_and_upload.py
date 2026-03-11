import csv
import requests
import io
import re
import os

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
MIN_QTY_PRIORITY = 16
STABILITY_PRICE_DELTA = 1.0


# -----------------------------
# Utility
# -----------------------------

def detect_delim(text):
    try:
        sample = text[:8192]
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return d.delimiter
    except:
        first = text.splitlines()[0]
        if "\t" in first: return "\t"
        if "|" in first: return "|"
        if ";" in first: return ";"
        return ","


def to_int(x, default=0):
    try:
        s=str(x or "").strip()
        if not s:
            return default
        return int(float(s.replace(",",".")))
    except:
        return default


def to_price(x, default=999999):

    try:
        s=str(x or "").strip()

        if not s:
            return default

        s=s.replace("€","")
        s=s.replace(",",".")
        s=re.sub(r"[^0-9.]", "", s)

        return float(s)

    except:
        return default


def supplier_from_sku(sku):

    parts=(sku or "").split("_")

    if len(parts)>=2 and parts[1].isdigit():
        return parts[1]

    for p in parts:
        if len(p)==4 and p.isdigit():
            return p

    return ""


def norm(s):
    return str(s or "").strip().lower()


def clean_text(text):

    t=str(text or "")

    t=re.sub("<.*?>"," ",t)
    t=t.replace("&nbsp;"," ")
    t=t.replace('"',"")
    t=t.replace("|"," ")
    t=t.replace("\n"," ")
    t=t.replace("\r"," ")

    t=re.sub(" +"," ",t)

    return t.strip()


# -----------------------------
# Leggi feed precedente
# -----------------------------

def load_previous_feed():

    previous={}

    if not os.path.exists(OUTPUT_FILE):
        return previous

    with open(OUTPUT_FILE,encoding="utf-8") as f:

        reader=csv.DictReader(f,delimiter="|")

        for r in reader:

            previous[r["ean"]] = {
                "sku": r["sku"],
                "price": to_price(r.get("prezzo_iva_esclusa"))
            }

    return previous


# -----------------------------
# Scelta SKU
# -----------------------------

def choose_product(rows):

    if len(rows)==1:
        return rows[0]

    rows_sorted=sorted(rows,key=lambda x:x["price"])

    min_price=rows_sorted[0]["price"]

    # 0373 entro +20€
    for r in rows:
        if r["supplier"]=="0373" and r["qty"]>=MIN_QTY_PRIORITY and r["price"]<=min_price+20:
            return r

    # 0382 minimo
    for r in rows:
        if r["supplier"]=="0382" and r["qty"]>=MIN_QTY_PRIORITY and r["price"]==min_price:
            return r

    # 0381 minimo
    for r in rows:
        if r["supplier"]=="0381" and r["qty"]>=MIN_QTY_PRIORITY and r["price"]==min_price:
            return r

    # 0372 / 0380
    candidates=[r for r in rows if r["supplier"] in {"0372","0380"} and r["qty"]>=MIN_QTY_PRIORITY]

    if candidates:
        return sorted(candidates,key=lambda x:x["price"])[0]

    return rows_sorted[0]


# -----------------------------
# MAIN
# -----------------------------

def main():

    previous_feed = load_previous_feed()

    print("Feed precedente caricato:",len(previous_feed))

    print("Scarico feed originale...")

    resp=requests.get(INPUT_URL)
    resp.raise_for_status()

    text=resp.content.decode("utf-8-sig",errors="replace")

    f=io.StringIO(text)

    delim=detect_delim(text)

    reader=csv.DictReader(f,delimiter=delim)

    fields=[
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso"
    ]

    products_by_ean={}

    for r in reader:

        sku=r.get("sku") or r.get("SKU") or ""

        supplier=supplier_from_sku(sku)

        if supplier not in ALLOWED_SUPPLIERS:
            continue

        cat1=norm(r.get("cat1") or r.get("categoria"))

        if cat1 not in ALLOWED_CAT1:
            continue

        titolo=norm(r.get("titolo_prodotto") or r.get("nome"))

        if any(x in titolo for x in EXCLUDE_TITLE_SUBSTRINGS):
            continue

        qty=to_int(r.get("quantita") or r.get("qty"))

        if qty<MIN_QTY:
            continue

        price=to_price(r.get("prezzo_iva_esclusa"))

        ean=r.get("ean") or ""

        row={
            "raw":r,
            "sku":sku,
            "supplier":supplier,
            "qty":qty,
            "price":price
        }

        products_by_ean.setdefault(ean,[]).append(row)


    print("EAN analizzati:",len(products_by_ean))

    rows_out=[]

    for ean,rows in products_by_ean.items():

        chosen=choose_product(rows)

        prev=previous_feed.get(ean)

        # stabilizzazione
        if prev:

            for r in rows:

                if r["sku"]==prev["sku"]:

                    if abs(r["price"]-chosen["price"])<STABILITY_PRICE_DELTA and r["qty"]>=MIN_QTY_PRIORITY:

                        chosen=r
                        break


        r=chosen["raw"]

        row={k: clean_text(r.get(k)) if k!="quantita" else chosen["qty"] for k in fields}

        rows_out.append(row)


    print("Prodotti finali:",len(rows_out))


    with open(OUTPUT_FILE,"w",encoding="utf-8",newline="") as out:

        writer=csv.DictWriter(
            out,
            fieldnames=fields,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            escapechar="\\"
        )

        writer.writeheader()

        for r in rows_out:
            writer.writerow(r)


    print("Feed generato:",OUTPUT_FILE)


if __name__=="__main__":
    main()
