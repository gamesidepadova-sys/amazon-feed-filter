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
SUPPLIER_PRIORITY = "0373"
MAX_DIFF_PRIORITY = 20


# ------------------------------------------------
# Utility
# ------------------------------------------------

def detect_delim(text):
    try:
        sample = text[:8192]
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except:
        return ","


def to_int(x):
    try:
        s=str(x).strip().replace(".", "").replace(",", ".")
        return int(float(s))
    except:
        return 0


def to_float(x):
    try:
        s=str(x).strip().replace(",", ".")
        return float(s)
    except:
        return 0.0


def supplier_from_sku(sku):
    m=re.search(r"(03[0-9]{2})", sku or "")
    return m.group(1) if m else ""


def norm(s):
    return str(s or "").strip().lower()


def clean_text(t):
    t=str(t or "")
    t=re.sub("<.*?>"," ",t)
    t=t.replace("&nbsp;"," ")
    t=t.replace('"',"")
    t=t.replace("|"," ")
    t=t.replace("\n"," ")
    t=t.replace("\r"," ")
    t=re.sub(" +"," ",t)
    return t.strip()


def valid_ean(e):
    if not e:
        return False
    e=e.strip()
    return e.isdigit() and 8<=len(e)<=14


# ------------------------------------------------
# Main
# ------------------------------------------------

def main():

    print("📥 Download feed...")

    resp=requests.get(INPUT_URL)
    resp.raise_for_status()

    text=resp.content.decode("utf-8-sig","replace")

    delim=detect_delim(text)

    reader=csv.DictReader(io.StringIO(text), delimiter=delim)

    fields=[
        "cat1","sku","ean","mpn","quantita","prezzo_iva_esclusa",
        "titolo_prodotto","immagine_principale","descrizione_prodotto",
        "costo_spedizione","cat2","cat3","marca","peso","tag"
    ]

    ean_dict=defaultdict(list)

    total_rows=0

    for r in reader:

        total_rows+=1

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

        if qty < MIN_QTY:
            continue

        ean=clean_text(r.get("ean"))

        if not valid_ean(ean):
            continue

        prezzo_raw=r.get("prezzo_iva_esclusa") or ""
        prezzo_num=to_float(prezzo_raw)

        row={}

        for k in fields:

            if k=="sku":
                continue

            if k=="quantita":
                row[k]=qty

            elif k=="prezzo_iva_esclusa":
                row[k]=clean_text(prezzo_raw)

            else:
                row[k]=clean_text(r.get(k))

        row["_supplier"]=supplier
        row["_price"]=prezzo_num

        ean_dict[ean].append(row)


    print("EAN validi:",len(ean_dict))


    rows_out=[]


    for ean,items in ean_dict.items():

        cheapest=min(items, key=lambda x:x["_price"])

        cheapest_price=cheapest["_price"]

        preferred=[x for x in items
                   if x["_supplier"]==SUPPLIER_PRIORITY
                   and x["_price"] <= cheapest_price + MAX_DIFF_PRIORITY]

        if preferred:
            chosen=min(preferred, key=lambda x:x["_price"])
        else:
            chosen=cheapest

        out={k:v for k,v in chosen.items() if not k.startswith("_")}

        # SKU = EAN
        out["sku"]=ean

        # TAG fornitore scelto
        out["tag"]=f"supplier_{chosen['_supplier']}"

        rows_out.append(out)


    if not rows_out:
        raise Exception("Feed vuoto")


    with open(OUTPUT_FILE,"w",encoding="utf-8",newline="") as f:

        writer=csv.DictWriter(
            f,
            fieldnames=fields,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
            escapechar="\\"
        )

        writer.writeheader()

        for r in rows_out:
            writer.writerow(r)


    print("✅ Feed generato:",OUTPUT_FILE)
    print("Prodotti finali:",len(rows_out))


if __name__=="__main__":
    main()
