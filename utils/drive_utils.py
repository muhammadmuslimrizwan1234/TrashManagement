import os
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# -------- Setup --------
ROOT = os.path.dirname(os.path.dirname(__file__))
CREDENTIALS_PATH = os.path.join(ROOT, "service_account.json")  # your GCP key

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE = None

def get_service():
    global SERVICE
    if SERVICE is None:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH, scopes=SCOPES
        )
        SERVICE = build("drive", "v3", credentials=creds)
    return SERVICE

# -------- Helpers --------
def get_folder_id(path, parent_id=None):
    """
    Get or create nested folders in Google Drive based on path (e.g. 'dataset/plastic').
    Returns final folder ID.
    """
    service = get_service()
    parts = path.strip("/").split("/")
    current_parent = parent_id or "root"

    for part in parts:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{part}' and '{current_parent}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])

        if files:
            current_parent = files[0]["id"]
        else:
            folder_metadata = {
                "name": part,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [current_parent],
            }
            folder = service.files().create(body=folder_metadata, fields="id").execute()
            current_parent = folder["id"]

    return current_parent

# -------- Upload --------
def upload_to_drive(local_path, drive_path):
    """
    Uploads a file to Google Drive into the given drive_path (folders auto-created).
    Example: upload_to_drive('models/model.h5', 'models/model.h5')
    """
    service = get_service()

    folder_path, filename = os.path.split(drive_path)
    folder_id = get_folder_id(folder_path) if folder_path else "root"

    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)

    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])

    if files:
        file_id = files[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"♻️ Updated {drive_path} in Drive")
    else:
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"⬆️ Uploaded {drive_path} to Drive")

# -------- Download --------
def download_from_drive(drive_path, local_path):
    """
    Downloads a file/folder from Google Drive.
    Example:
      download_from_drive("models/model.h5", "./models/model.h5")
      download_from_drive("dataset", "./dataset")
    """
    service = get_service()

    # Folder case
    if not os.path.splitext(drive_path)[1]:
        folder_id = get_folder_id(drive_path)
        os.makedirs(local_path, exist_ok=True)

        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()

        for item in results.get("files", []):
            file_id, name, mime = item["id"], item["name"], item["mimeType"]
            dest = os.path.join(local_path, name)

            if mime == "application/vnd.google-apps.folder":
                download_from_drive(f"{drive_path}/{name}", dest)
            else:
                request = service.files().get_media(fileId=file_id)
                with io.FileIO(dest, "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                print(f"⬇️ Downloaded {drive_path}/{name} -> {dest}")
    else:
        # File case
        folder_path, filename = os.path.split(drive_path)
        folder_id = get_folder_id(folder_path) if folder_path else "root"

        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])

        if not files:
            print(f"⚠️ {drive_path} not found in Drive")
            return

        file_id = files[0]["id"]
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        request = service.files().get_media(fileId=file_id)
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        print(f"⬇️ Downloaded {drive_path} -> {local_path}")
