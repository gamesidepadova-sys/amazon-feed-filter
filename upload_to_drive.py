from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

FILE_NAME = "filtered_clean.csv"      # file generato dal filtro
DRIVE_FILE_NAME = "filtered.csv"      # nome su Google Drive
DRIVE_FOLDER_ID = None                # opzionale: ID cartella

def main():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)

    media = MediaFileUpload(FILE_NAME, mimetype="text/csv", resumable=False)

    # Cerca file esistente
    query = f"name='{DRIVE_FILE_NAME}' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if files:
        file_id = files[0]["id"]
        service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        print("File aggiornato su Drive:", DRIVE_FILE_NAME)
    else:
        metadata = {"name": DRIVE_FILE_NAME}
        if DRIVE_FOLDER_ID:
            metadata["parents"] = [DRIVE_FOLDER_ID]

        service.files().create(
            body=metadata,
            media_body=media,
            fields="id"
        ).execute()
        print("File creato su Drive:", DRIVE_FILE_NAME)

if __name__ == "__main__":
    main()
