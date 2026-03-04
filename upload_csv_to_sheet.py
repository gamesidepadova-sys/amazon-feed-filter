import csv
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import re

# =========================
# CONFIGURAZIONE
# =========================
INPUT_FILE = "source.csv"
OUTPUT_FILE = "filtered.csv"

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

# Drive
SERVICE_ACCOUNT_FILE = "service_account.json"  # Path al tuo service account
DRIVE_FOLDER_ID = "ID_cartella_drive"          # Cartella su Drive dove mettere filtered.csv

# =========================
# FUNZIONI DI SUPPORTO
# =========================
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
    """Rimuove caratteri invisibili, HTML, doppi apici, uniforma newline"""
    if not s:
        return ""
    s = re.sub(r'[\x00-\x1F]', '', s)        # caratteri invisibili
    s = s.replace('""', "'")                  # doppi apici
    s = re.sub(r'<[^>]+>', '', s)            # tag HTML
    s = s.replace('\r\n', '\n')              # newline uniformi
    s = s.replace('\r', '\n')
    return s.strip()

# =========================
# FILTRAGGIO E PULIZIA CSV
# =========================
def filter_csv():
    raw = Path(INPUT_FILE).read_bytes()
    text = raw.decode("utf-8-sig", errors="replace")
    delim = detect_delim(text)

    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    if not reader.fieldnames:
        raise RuntimeError(f"{INPUT_FILE} has no header row")

    required = {"cat1", "sku", "quantita", "prezzo_iva_esclusa", "titolo_prodotto"}
    missing = [c for c in required if c not in set(reader.fieldnames)]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}. Header={reader.fieldnames}")

    rows_in = 0
    rows_out = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=reader.fieldnames,
            delimiter=delim,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()

        for row in reader:
            rows_in += 1
            sku = (row.get("sku") or "").strip()
            if not sku:
                continue

            # Supplier
            supplier = supplier_from_sku(sku)
            if supplier not in ALLOWED_SUPPLIERS:
                continue

            # Categoria
            cat1 = norm(row.get("cat1"))
            if cat1 not in ALLOWED_CAT1:
                continue

            # Quantità
            qty = to_int(row.get("quantita"))
            if qty < MIN_QTY:
                continue

            # Esclusione titolo
            title = norm(row.get("titolo_prodotto"))
            if any(sub in title for sub in EXCLUDE_TITLE_SUBSTRINGS):
                continue

            # --- PULIZIA TESTI EXTRA ---
            for k in row:
                row[k] = clean_text(row[k])

            writer.writerow(row)
            rows_out += 1

    print("Filtered & Cleaned CSV ready!")
    print("Detected delimiter:", repr(delim))
    print(f"Rows read: {rows_in}")
    print(f"Rows written: {rows_out}")
    return OUTPUT_FILE

# =========================
# UPLOAD SU DRIVE
# =========================
def upload_to_drive(file_path):
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("drive", "v3", credentials=creds)

    # Controlla se il file esiste già
    res = service.files().list(
        q=f"'{DRIVE_FOLDER_ID}' in parents and name='{Path(file_path).name}' and trashed=false",
        fields="files(id, name)"
    ).execute()

    media = MediaFileUpload(file_path, mimetype="text/csv", resumable=True)

    if res.get("files"):
        file_id = res["files"][0]["id"]
        updated = service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        print(f"File aggiornato su Drive: {updated['name']} (ID {updated['id']})")
    else:
        new_file = service.files().create(
            body={"name": Path(file_path).name, "parents": [DRIVE_FOLDER_ID]},
            media_body=media,
            fields="id, name"
        ).execute()
        print(f"File creato su Drive: {new_file['name']} (ID {new_file['id']})")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    filtered_file = filter_csv()
    upload_to_drive(filtered_file)
