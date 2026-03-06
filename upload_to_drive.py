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

    # FORZA un cambiamento per test
    with open("feed_poleepo.csv", "a") as f:
        f.write("\n# forced upload test\n")

    media = MediaFileUpload(
        "feed_poleepo.csv",
        mimetype="text/csv",
        resumable=False
    )

    print("Uploading feed_poleepo.csv to Drive...")
    result = service.files().update(
        fileId=FILE_ID,
        media_body=media
    ).execute()
    print("Response from Drive API:", result)

if __name__ == "__main__":
    main()
