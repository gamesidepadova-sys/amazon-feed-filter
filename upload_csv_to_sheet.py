#!/usr/bin/env python3
import csv
import os
from typing import List, Dict, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

INPUT_CSV = os.environ.get("FILTERED_LOCAL_FILE", "filtered.csv").strip()

# Colonne che vogliamo vedere nel Google Sheet "Filtered" (operative e leggere)
# (escludiamo descrizione_prodotto / immagini per evitare celle > 50k)
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

MAX_CELL_CHARS = 49000  # Google Sheets: 50k per cella -> stiamo sotto
CHUNK_ROWS = 500        # scrittura a blocchi


def detect_delimiter(first_line: str) -> str:
    # euristica semplice e stabile
    if "\t" in first_line:
        return "\t"
    if "|" in first_line:
        return "|"
    if ";" in first_line:
        return ";"
    return ","


def safe_cell(x: Any) -> str:
    s = "" if x is None else str(x)
    if len(s) > MAX_CELL_CHARS:
        return s[: MAX_CELL_CHARS - 1] + "…"
    return s


def normalize_fieldnames(fieldnames: List[str]) -> List[str]:
    return [str(h or "").strip() for h in fieldnames]


def main():
    sheet_id = os.environ.get("FILTERED_SHEET_ID", "").strip()
    tab_name = os.environ.get("FILTERED_TAB_NAME", "Foglio1").strip()

    if not sheet_id:
        raise RuntimeError("FILTERED_SHEET_ID env var is missing or empty")

    if not os.path.exists("sa.json"):
        raise RuntimeError("sa.json not found. Ensure the workflow writes the service account JSON to sa.json.")

    # Google Sheets client
    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    sheets = build("sheets", "v4", credentials=creds)

    # Read CSV and build values matrix
    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)
        delim = detect_delimiter(first)

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError(f"{INPUT_CSV} has no header row")

        fieldnames = normalize_fieldnames(reader.fieldnames)
        norm_map = {h.strip().lower(): h for h in fieldnames}  # lower -> original

        # keep only columns that exist in the csv (case-insensitive)
        header_out: List[str] = []
        for k in KEEP_COLUMNS:
            if k.lower() in norm_map:
                header_out.append(norm_map[k.lower()])  # keep original spelling from file

        # ensure sku exists
        if "sku" not in [h.strip().lower() for h in header_out]:
            raise RuntimeError(f"{INPUT_CSV} does not contain required column 'sku'. Header={fieldnames}")

        values: List[List[str]] = []
        # write header using normalized standard names (not original)
        # -> this makes the Sheet stable even if upstream header casing changes
        std_header = [h.strip().lower() for h in header_out]
        values.append(std_header)

        rows = 0
        for row in reader:
            out_row: List[str] = []
            for orig_name in header_out:
                out_row.append(safe_cell(row.get(orig_name, "")))
            values.append(out_row)
            rows += 1

    # Clear destination
    sheets.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"{tab_name}!A1:ZZ",
        body={}
    ).execute()

    # Write in chunks
    def write_chunk(start_row_1based: int, chunk_vals: List[List[str]]):
        rng = f"{tab_name}!A{start_row_1based}"
        sheets.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": chunk_vals},
        ).execute()

    start = 0
    row_cursor = 1
    while start < len(values):
        chunk = values[start:start + CHUNK_ROWS]
        write_chunk(row_cursor, chunk)
        row_cursor += len(chunk)
        start += CHUNK_ROWS

    print(f"Uploaded {INPUT_CSV} -> Google Sheet {sheet_id} tab={tab_name}")
    print(f"Delimiter detected: {repr(delim)}")
    print(f"Rows written: {rows} (+ header)")
    print(f"Columns written: {len(values[0])} -> {values[0]}")


if __name__ == "__main__":
    main()
