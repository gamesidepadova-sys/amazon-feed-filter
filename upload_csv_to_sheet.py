import csv
import os
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build

# Colonne da pubblicare nel Google Sheet "Filtered"
# (togliamo descrizione_prodotto / immagine_principale per evitare celle > 50k)
KEEP_COLUMNS = [
    "cat1",
    "sku",
    "ean",
    "mpn",
    "quantita",
    "prezzo_iva_esclusa",
    "titolo_prodotto",
    "costo_spedizione",
]

MAX_CELL_CHARS = 49000  # sotto il limite 50k
CHUNK_ROWS = 500        # blocchi per API

INPUT_CSV = "filtered.csv"


def detect_delim(first_line: str) -> str:
    if "\t" in first_line:
        return "\t"
    if "|" in first_line:
        return "|"
    if ";" in first_line:
        return ";"
    return ","


def safe_cell(x: str) -> str:
    s = "" if x is None else str(x)
    if len(s) > MAX_CELL_CHARS:
        return s[: MAX_CELL_CHARS - 1] + "…"
    return s


def main():
    sheet_id = os.environ.get("FILTERED_SHEET_ID", "").strip()
    tab_name = os.environ.get("FILTERED_TAB_NAME", "Foglio1").strip()  # nel tuo caso Foglio1
    if not sheet_id:
        raise RuntimeError("FILTERED_SHEET_ID env var missing")

    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    sheets = build("sheets", "v4", credentials=creds)

    # Leggi CSV
    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
        first = f.readline()
        f.seek(0)
        delim = detect_delim(first)

        reader = csv.DictReader(f, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError("filtered.csv has no header")

        # Normalizza: alcune volte header può avere spazi
        fieldnames = [h.strip() for h in reader.fieldnames]
        # Mappa originale -> pulito
        orig_map = {h.strip(): h for h in reader.fieldnames}

        # Prepara header output
        header_out: List[str] = []
        for k in KEEP_COLUMNS:
            # se la colonna non esiste, la saltiamo
            if k in fieldnames:
                header_out.append(k)

        if "sku" not in header_out:
            raise RuntimeError(f"filtered.csv non contiene la colonna sku. Header={fieldnames}")

        values: List[List[str]] = []
        values.append(header_out)

        for row in reader:
            out_row = []
            for k in header_out:
                v = row.get(orig_map.get(k, k), "")
                out_row.append(safe_cell(v))
            values.append(out_row)

    # 1) Clear tab
    sheets.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"{tab_name}!A1:ZZ",
        body={}
    ).execute()

    # 2) Write in chunks
    def write_chunk(start_row_1based: int, chunk_vals: List[List[str]]):
        rng = f"{tab_name}!A{start_row_1based}"
        sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": chunk_vals},
        ).execute()

    # Scrivi header + righe
    # chunk 1 include header
    start = 0
    row_cursor = 1
    while start < len(values):
        chunk = values[start : start + CHUNK_ROWS]
        write_chunk(row_cursor, chunk)
        row_cursor += len(chunk)
        start += CHUNK_ROWS

    print(f"Uploaded to Google Sheet: {sheet_id} tab={tab_name}")
    print(f"Rows written: {len(values)-1} (plus header)")
    print(f"Columns written: {len(values[0])} -> {values[0]}")


if __name__ == "__main__":
    main()
