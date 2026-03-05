import csv
import requests
import re
from io import StringIO

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


def detect_delim(text):
    try:
        sample = text[:8192]
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return d.delimiter
    except:
        return "|"


def to_int(x, default=0):
    try:
        s = str(x or "").strip()
        if not s:
            return default
        return int(float(s.replace(",", ".")))
    except:
        return default


def supplier_from_sku(sku):
    parts = (sku or "").split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""


def norm(s):
    return str(s or "").strip().lower()


def clean_text(s):
    s = str(s or "")
    s = re.sub(r"[\x00-\x1F]", "", s)
    s = s.replace('""', "'")
    s = s.replace("\r\n", " ")
    s = s.replace("\r", " ")
    s = s.replace("\n", " ")
    return s


def main():

    resp = requests.get(INPUT_URL)
    resp.raise_for_status()

    text = resp.content.decode("utf-8-sig", errors="replace")

    delim = detect_delim(text)

    # PARSING CORRETTO CSV MULTILINEA
    reader = csv.DictReader(StringIO(text), delimiter=delim)

    rows_in = 0
    rows_out = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:

        writer = csv.DictWriter(
            fout,
            fieldnames=reader.fieldnames,
            delimiter="|",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )

        writer.writeheader()

        for row in reader:

            rows_in += 1

            sku = (row.get("sku") or "").strip()
            if not sku:
                continue

            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            cat1 = norm(row.get("cat1"))
            if cat1 not in ALLOWED_CAT1:
                continue

            qty = to_int(row.get("quantita"))
            if qty < MIN_QTY:
                continue

            title = norm(row.get("titolo_prodotto"))
            if any(substr in title for substr in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            cleaned = {k: clean_text(v) for k, v in row.items()}

            writer.writerow(cleaned)

            rows_out += 1

    print(f"rows_in={rows_in} rows_out={rows_out}")


if __name__ == "__main__":
    main()
