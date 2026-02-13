import csv
import os
from pathlib import Path
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build

INPUT_CSV = os.environ.get("INPUT_CSV", "filtered.csv")
SHEET_ID = os.environ.get("FILTERED_SHEET_ID", "").strip()
SHEET_NAME = os.environ.get("FILTERED_SHEET_NAME", "Filtered").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def detect_delim(first_line: str) -> str:
    if "\t" in first_line:
        return "\t"
    if "|" in first_line:
        return "|"
    if ";" in first_line:
        return ";"
    return ","


def read_csv_rows(path: str) -> List[List[str]]:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"CSV not found: {path}")

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        first = f.readline()
        f.seek(0)
        delim = detect_delim(first)
        r = csv.reader(f, delimiter=delim)
        rows = [list(row) for row in r]

    if not rows:
        raise RuntimeError("CSV is empty")

    return rows


def main():
    if not SHEET_ID:
        raise RuntimeError("FILTERED_SHEET_ID env var is missing or empty")

    rows = read_csv_rows(INPUT_CSV)

    creds = service_account.Credentials.from_service_account_file("sa.json", scopes=SCOPES)
    sheets = build("sheets", "v4", credentials=creds)

    # Pulisce e riscrive tutto (semplice e robusto)
    # Se vuoi preservare formattazioni, possiamo migliorarlo dopo.
    sheets.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A:Z"
    ).execute()

    sheets.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    print(f"Uploaded {INPUT_CSV} -> Google Sheet {SHEET_ID} ({SHEET_NAME}), rows={len(rows)} cols={len(rows[0])}")


if __name__ == "__main__":
    main()
