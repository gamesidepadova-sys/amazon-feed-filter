#!/usr/bin/env python3
import csv
import os
import time
from typing import List, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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

# Con batchUpdate possiamo inviare range grandi in una singola request.
# Se il dataset è enorme, facciamo pochissimi batch (es. 5) per stare safe.
MAX_ROWS_PER_BATCH = 20000  # 2 batch per ~44k, ok


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


def retry(call_fn, max_tries=6):
    delay = 2
    for i in range(max_tries):
        try:
            return call_fn()
        except HttpError as e:
            status = getattr(e, "status_code", None)
            # googleapiclient sometimes stores status in resp
            if hasattr(e, "resp") and e.resp is not None:
                status = e.resp.status
            if status in (429, 500, 503):
                if i == max_tries - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            raise


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

    # --- metadata & tab resolve ---
    meta = retry(lambda: sheets.spreadsheets().get(spreadsheetId=sheet_id).execute())
    sheet_objs = meta.get("sheets", [])
    if not sheet_objs:
        raise RuntimeError("Spreadsheet has no sheets/tabs")

    title_to_sheet = {s["properties"]["title"]: s for s in sheet_objs}
    if tab_name not in title_to_sheet:
        real = sheet_objs[0]["properties"]["title"]
        print(f"WARNING: tab '{tab_name}' not found. Using first tab: '{real}'")
        tab_name = real

    tab = title_to_sheet.get(tab_name) or sheet_objs[0]
    props = tab["properties"]
    sheet_title = props["title"]
    sheet_gid = props["sheetId"]
    grid = props.get("gridProperties", {})
    current_rows = int(grid.get("rowCount", 1000))
    current_cols = int(grid.get("columnCount", 26))

    # --- read csv -> values ---
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

        # stable lowercase header in sheet
        std_header = [h.lower() for h in header_out]

        values: List[List[str]] = [std_header]
        rows = 0
        for row in reader:
            out_row: List[str] = []
            for orig_name in header_out:
                out_row.append(safe_cell(row.get(orig_name, "")))
            values.append(out_row)
            rows += 1

    needed_rows = len(values)
    needed_cols = len(values[0])

    # --- resize grid if needed ---
    new_rows = max(current_rows, needed_rows)
    new_cols = max(current_cols, needed_cols)
    if new_rows != current_rows or new_cols != current_cols:
        print(f"Resizing sheet '{sheet_title}' grid: rows {current_rows}->{new_rows}, cols {current_cols}->{new_cols}")
        retry(lambda: sheets.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_gid,
                                "gridProperties": {"rowCount": new_rows, "columnCount": new_cols},
                            },
                            "fields": "gridProperties(rowCount,columnCount)",
                        }
                    }
                ]
            },
        ).execute())

    # --- clear ---
    clear_range = f"{sheet_title}!A:ZZ"
    retry(lambda: sheets.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=clear_range,
        body={}
    ).execute())

    # --- write using batchUpdate in VERY FEW requests ---
    # We send multiple ranges in one batchUpdate call if needed.
    data = []
    start_row = 1  # 1-based
    idx = 0
    while idx < len(values):
        chunk = values[idx: idx + MAX_ROWS_PER_BATCH]
        rng = f"{sheet_title}!A{start_row}"
        data.append({"range": rng, "values": chunk})
        start_row += len(chunk)
        idx += MAX_ROWS_PER_BATCH

    def do_batch_write():
        return sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "valueInputOption": "RAW",
                "data": data
            }
        ).execute()

    retry(do_batch_write)

    print(f"Uploaded {INPUT_CSV} -> Google Sheet {sheet_id} tab={sheet_title}")
    print(f"Delimiter detected: {repr(delim)}")
    print(f"Rows written: {rows} (+ header)")
    print(f"Columns written: {needed_cols} -> {values[0]}")
    print(f"Batch ranges sent: {len(data)} (MAX_ROWS_PER_BATCH={MAX_ROWS_PER_BATCH})")


if __name__ == "__main__":
    main()
