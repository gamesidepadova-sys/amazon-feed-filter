import csv
import requests
import re
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from io import StringIO

# ----------------------
# CONFIG
# ----------------------
INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "filtered_clean.csv"

SERVICE_ACCOUNT_FILE = "service_account.json"
DRIVE_FOLDER_ID = "ID_CARTELLA_DRIVE"
SCOPES = ['https://www.googleapis.com/auth/drive.file']

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

# ----------------------
# SUPPORT
# ----------------------
def detect_delim(text):
    try:
        sample = text[:8192]
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return d.delimiter
    except:
        return "|"

def supplier_from_sku(sku):
    parts = sku.split("_")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    for p in parts:
        if len(p) == 4 and p.isdigit():
            return p
    return ""

def clean_text(s):
    s = str(s or "")
    s = re.sub(r"[\x00-\x1F]", "", s)
    return s

# ----------------------
# UPLOAD
# ----------------------
def upload_to_drive(file_path, filename):
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=credentials)

    metadata = {'name': filename, 'parents': [DRIVE_FOLDER_ID]}
    media = MediaFileUpload(file_path, mimetype='text/csv')

    file = service.files().create(
        body=metadata, media_body=media, fields='id'
    ).execute()

    print("Caricato su Drive:", file.get("id"))

# ----------------------
# MAIN
# ----------------------
def main():
    # Scarica CSV
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")

    delim = detect_delim(text)
    reader = csv.DictReader(StringIO(text), delimiter=delim)

    # Le 10 colonne finali corrette
    FIELDNAMES = [
        "cat1",
        "sku",
        "ean",
        "mpn",
        "quantita",
        "prezzo_iva_esclusa",
        "titolo_prodotto",
        "immagine_principale",
        "descrizione_prodotto",
        "costo_spedizione"
    ]

    rows_out = []

    for row in reader:
        sku = (row.get("sku") or "").strip()
        if not sku:
            continue

        supplier = supplier_from_sku(sku)
        if supplier not in ALLOWED_SUPPLIERS:
            continue

        cat1 = (row.get("cat1") or "").strip().lower()
        if cat1 not in ALLOWED_CAT1:
            continue

        try:
            qty = int(float((row.get("quantita") or "0").replace(",", ".")))
        except:
            qty = 0
        if qty < MIN_QTY:
            continue

        title = (row.get("titolo_prodotto") or "").lower()
        if any(bad in title for bad in EXCLUDE_TITLE_SUBSTRINGS):
            continue

        cleaned = {k: clean_text(row.get(k, "")) for k in FIELDNAMES}
        rows_out.append(cleaned)

    # Scrivi CSV pipe senza colonna extra
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter="|", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows_out)

    print("Creato:", OUTPUT_FILE, "righe:", len(rows_out))

    upload_to_drive(OUTPUT_FILE, OUTPUT_FILE)

# ----------------------
if __name__ == "__main__":
    main()
