import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def main():
    file_id = os.environ.get("FILE_ID", "").strip()
    local_file = os.environ.get("LOCAL_FILE", "").strip()

    if not file_id:
        raise RuntimeError("FILE_ID env var is missing or empty")
    if not local_file:
        raise RuntimeError("LOCAL_FILE env var is missing or empty")
    if not os.path.exists(local_file):
        raise RuntimeError(f"Local file not found: {local_file}")

    creds = service_account.Credentials.from_service_account_file(
        "sa.json",
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    service = build("drive", "v3", credentials=creds)

    media = MediaFileUpload(
        local_file,
        resumable=True
    )

    service.files().update(
        fileId=file_id,
        media_body=media
    ).execute()

    print(f"Uploaded {local_file} to Drive (fileId={file_id})")

if __name__ == "__main__":
    main()
