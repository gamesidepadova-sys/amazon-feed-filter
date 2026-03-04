import csv
import requests
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# ----------------------
# CONFIG
# ----------------------
INPUT_URL = "http://listini.sellrapido.com/wh/_export_informaticatech_it.csv"
OUTPUT_FILE = "filtered_clean.csv"

SERVICE_ACCOUNT_FILE = "path/to/service_account.json"
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
    # Scarica CSV originale
    resp = requests.get(INPUT_URL)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig", errors="replace")

    # Rileva delimitatore
    sample = text[:8192]
    try:
        delim = csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except:
        delim = "|"

    # Leggi CSV in DataFrame
    df = pd.read_csv(
        pd.compat.StringIO(text),
        sep=delim,
        dtype=str,
        keep_default_na=False
    )

    # Filtri
    df["quantita"] = pd.to_numeric(df["quantita"], errors="coerce").fillna(0).astype(int)
    df = df[df["quantita"] >= MIN_QTY]

    df["cat1_norm"] = df["cat1"].str.lower().str.strip()
    df = df[df["cat1_norm"].isin(ALLOWED_CAT1)]

    df["supplier"] = df["sku"].str.extract(r"_(\d{4})")
    df = df[df["supplier"].isin(ALLOWED_SUPPLIERS)]

    df["title_norm"] = df["titolo_prodotto"].str.lower()
    for bad in EXCLUDE_TITLE_SUBSTRINGS:
        df = df[~df["title_norm"].str.contains(bad)]

    # Colonne finali (10 esatte)
    cols = [
        "cat1",
        "sku",
        "ean",
        "mpn",
        "quantita",
        "prezzo_iva_esclusa",
        "titolo_prodotto",
        "immagine_principale",
        "descrizione_prodotto",
        "costo_spedizione",
    ]

    df = df[cols]

    # Salva con pipe, senza colonna extra
    df.to_csv(OUTPUT_FILE, index=False, sep="|", quoting=csv.QUOTE_MINIMAL)

    print("Creato:", OUTPUT_FILE, "righe:", len(df))

    upload_to_drive(OUTPUT_FILE, OUTPUT_FILE)

# ----------------------
if __name__ == "__main__":
    main()
