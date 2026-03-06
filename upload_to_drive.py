from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

FILE_ID = os.environ["FILE_ID"]

def main():
    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    service = build("drive", "v3", credentials=creds)

    media = MediaFileUpload(
        "feed_poleepo.csv",
        mimetype="text/csv",
        resumable=False
    )

    service.files().update(
        fileId=FILE_ID,
        media_body=media
    ).execute()

    print("File aggiornato su Google Drive!")

if __name__ == "__main__":
    main()
