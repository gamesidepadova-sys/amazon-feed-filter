import csv
import re
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ----------------------
# Configurazione
# ----------------------
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
EXCLUDE_TITLE_SUBSTRINGS = {"phs-memory", "montatura"}  # case insensitive
MIN_QTY = 10

# Google Drive
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
DRIVE_FILE_NAME = "filtered_clean.csv"
DRIVE_FOLDER_ID = None  # opzionale: ID cartella

# ----------------------
# Funzioni di supporto
# ----------------------
def detect_delim(text: str) -> str:
    try:
        sample = text[:8192]
        d = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return d.delimiter
    except Exception:
        first = text.splitlines()[0] if text else ""
        if "\t" in first:
            return "\t"
        if "|" in first:
            return "|"
        if ";" in first and first.count(";") > first.count(","):
            return ";"
        return ","

def to_int(x, default=0) -> int:
    try:
        s = str(x or "").strip()
        if not s:
            return default
        return int(float(s.replace(",", ".")))
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

def clean_text(s: str) -> str:
    s = str(s or "")
    s = re.sub(r"[\x00-\x1F]", "", s)
    s = s.replace('""', "'")
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("\r\n", "\n")
    return s

# ----------------------
# Funzioni principali
# ----------------------
def filter_csv():
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")
    delim = detect_delim(text)

    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    if not reader.fieldnames:
        raise RuntimeError("CSV scaricato senza header")

    required = {"cat1", "sku", "quantita", "prezzo_iva_esclusa", "titolo_prodotto"}
    missing = [c for c in required if c not in set(reader.fieldnames)]
    if missing:
        raise RuntimeError(f"Colonne mancanti: {missing}")

    rows_in = 0
    rows_out = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(
            fout, fieldnames=reader.fieldnames, delimiter=delim,
            lineterminator="\n", quoting=csv.QUOTE_MINIMAL
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

            cleaned_row = {k: clean_text(v) for k, v in row.items()}
            writer.writerow(cleaned_row)
            rows_out += 1

    print(f"CSV filtrato pronto! Rows in: {rows_in}, Rows out: {rows_out}")
    return OUTPUT_FILE

def upload_to_drive(file_path: str):
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("drive", "v3", credentials=creds)
    media = MediaFileUpload(file_path, mimetype="text/csv", resumable=False)

    query = f"name='{DRIVE_FILE_NAME}' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if files:
        file_id = files[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"File aggiornato su Drive: {DRIVE_FILE_NAME}")
    else:
        metadata = {"name": DRIVE_FILE_NAME}
        if DRIVE_FOLDER_ID:
            metadata["parents"] = [DRIVE_FOLDER_ID]
        service.files().create(body=metadata, media_body=media, fields="id").execute()
        print(f"File creato su Drive: {DRIVE_FILE_NAME}")

if __name__ == "__main__":
    filtered_file = filter_csv()
    upload_to_drive(filtered_file)
