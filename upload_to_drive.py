from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

# -----------------------------
# Configurazione
# -----------------------------
SERVICE_ACCOUNT_FILE = "service_account.json"  # già creato dal workflow
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# ID del file su Drive da aggiornare
# Se vuoi creare un nuovo file, metti None
DRIVE_FILE_ID = "1mJ3sHcF5w7eXxV3kLc9sI4XN7afjfgGB"

# CSV locale da caricare
LOCAL_CSV = "filtered.csv"

# -----------------------------
# Autenticazione
# -----------------------------
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("drive", "v3", credentials=creds)

# -----------------------------
# Upload
# -----------------------------
if not os.path.exists(LOCAL_CSV):
    raise FileNotFoundError(f"{LOCAL_CSV} non trovato!")

media = MediaFileUpload(LOCAL_CSV, mimetype="text/csv", resumable=True)

if DRIVE_FILE_ID:
    # Aggiorna file esistente
    file = service.files().update(
        fileId=DRIVE_FILE_ID,
        media_body=media
    ).execute()
    print(f"Aggiornato file Drive: {file.get('name')} (ID={file.get('id')})")
else:
    # Crea nuovo file
    file_metadata = {"name": os.path.basename(LOCAL_CSV)}
    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,name"
    ).execute()
    print(f"Creato nuovo file Drive: {file.get('name')} (ID={file.get('id')})")
