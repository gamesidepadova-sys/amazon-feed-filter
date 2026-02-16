#!/usr/bin/env python3
import csv
import os
from typing import List, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build

INPUT_CSV = os.environ.get("FILTERED_LOCAL_FILE", "filtered.csv").strip()

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

MAX_CELL_CHARS = 49000
CHUNK_ROWS = 500  # scrittura a blocchi


def detect_delimiter(first_line: str) -> str:
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


def main():
    sheet_id = os.environ.get("FILTERED_SHEET_ID", "").strip()
    tab_name = os.environ.get("FILTERED_TAB_NAME", "Filtered").strip()

    if not sheet_id:
        raise RuntimeError("FILTERED_SHEET_ID env var is missing or empty")

    if not os.path.exists("sa.json"):
        raise RuntimeError("sa.json not found")

    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    sheets = build("sheets", "v4", credentials=creds)

    # --- Spreadsheet metadata & resolve tab ---
    meta = sheets.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_objs = meta.get("sheets", [])
    if not sheet_objs:
        raise RuntimeError("Spreadsheet has no sheets/tabs")

    title_to_sheet = {s["properties"]["title"]: s for s in sheet_objs}
    if tab_name not in title_to_sheet:
        real = sheet_objs[0]["properties"]["title"]
        print(f"WARNING: tab '{tab_name}' not found. Using first tab: '{real}'")
        tab_name = real

    tab = title_to_sheet.get(tab_name) or sheet_objs[0]
    sheet_props = tab["properties"]
    sheet_title = sheet_props["title"]
    sheet_gid = sheet_props["sheetId"]
    grid = sheet_props.get("gridProperties", {})
    current_rows = int(grid.get("rowCount", 1000))
    current_cols = int(grid.get("columnCount", 26))

    # --- Read CSV -> values ---
    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as fin:
        first = fin.readline()
        fin.seek(0)
        delim = detect_delimiter(first)

        reader = csv.DictReader(fin, delimiter=delim)
        if not reader.fieldnames:
            raise RuntimeError(f"{INPUT_CSV} has no header row")

        fieldnames = [h.strip() for h in reader.fieldnames]
        norm_map = {h.lower(): h for h in fieldnames}

        header_out: List[str] = []
        for k in KEEP_COLUMNS:
            if k.lower() in norm_map:
                header_out.append(norm_map[k.lower()])

        if "sku" not in [h.lower() for h in header_out]:
            raise RuntimeError(f"{INPUT_CSV} missing required column 'sku'. Header={fieldnames}")

        std_header = [h.lower() for h in header_out]

        values: List[List[str]] = [std_header]
        rows = 0
        for row in reader:
            out_row: List[str] = []
            for orig_name in header_out:
                out_row.append(safe_cell(row.get(orig_name, "")))
            values.append(out_row)
            rows += 1

    needed_rows = len(values)  # header + data
    needed_cols = len(values[0])

    # --- Ensure grid is big enough ---
    new_rows = max(current_rows, needed_rows)
    new_cols = max(current_cols, needed_cols)

    if new_rows != current_rows or new_cols != current_cols:
        print(f"Resizing sheet '{sheet_title}' grid: rows {current_rows}->{new_rows}, cols {current_cols}->{new_cols}")
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_gid,
                                "gridProperties": {
                                    "rowCount": new_rows,
                                    "columnCount": new_cols,
                                },
                            },
                            "fields": "gridProperties(rowCount,columnCount)",
                        }
                    }
                ]
            },
        ).execute()

    # --- Clear destination (valid range) ---
    clear_range = f"{sheet_title}!A:ZZ"
    sheets.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=clear_range,
        body={}
    ).execute()

    # --- Write in chunks ---
    def write_chunk(start_row_1based: int, chunk_vals: List[List[str]]):
        rng = f"{sheet_title}!A{start_row_1based}"
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

    print(f"Uploaded {INPUT_CSV} -> Google Sheet {sheet_id} tab={sheet_title}")
    print(f"Delimiter detected: {repr(delim)}")
    print(f"Rows written: {rows} (+ header)")
    print(f"Columns written: {needed_cols} -> {values[0]}")


if __name__ == "__main__":
    main()
