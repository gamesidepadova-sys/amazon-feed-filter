from google.oauth2 import service_account
from googleapiclient.discovery import build

# ----------------------
# Configurazione
# ----------------------
SERVICE_ACCOUNT_FILE = "service_account.json"  # secret scritto dal workflow
CSV_FILE = "filtered_clean.csv"
DRIVE_FILE_ID = "1mJ3sHcF5w7eXxV3kLc9sI4XN7afjfgGB"  # ID del file Drive da aggiornare

SCOPES = ["https://www.googleapis.com/auth/drive"]

def main():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)

    # Leggi CSV
    with open(CSV_FILE, "rb") as f:
        content = f.read()

    # Aggiorna file su Drive
    file_metadata = {}
    media = {"body": content, "mimeType": "text/csv"}
    updated = service.files().update(
        fileId=DRIVE_FILE_ID,
        media_body=CSV_FILE
    ).execute()

    print(f"File Drive aggiornato: {DRIVE_FILE_ID}")

if __name__ == "__main__":
    main()
