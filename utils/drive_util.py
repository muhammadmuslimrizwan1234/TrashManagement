from dotenv import load_dotenv
load_dotenv()

import os
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# -------- Setup --------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]  # 🔹 Read-only
SERVICE = None

# Root folder (shared drive folder ID instead of "root")
DRIVE_ROOT = os.getenv("DRIVE_FOLDER_ID", "root")


def get_service():
    global SERVICE
    if SERVICE is None:
        # Load credentials from env
        service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT_JSON"))
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        SERVICE = build("drive", "v3", credentials=creds)
    return SERVICE


# -------- Helpers --------
def get_folder_id(path, parent_id=None):
    """
    Get (or create, if running locally with upload) nested folders in Google Drive.
    Always starts from DRIVE_ROOT.
    """
    service = get_service()
    current_parent = parent_id or DRIVE_ROOT

    if not path.strip():
        return current_parent

    parts = path.strip("/").split("/")
    for part in parts:
        query = (
            f"mimeType='application/vnd.google-apps.folder' "
            f"and name='{part}' and '{current_parent}' in parents and trashed=false"
        )
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get("files", [])

        if files:
            current_parent = files[0]["id"]
        else:
            # ❌ On Railway we never create, only local
            raise FileNotFoundError(f"Folder '{part}' not found in Drive path: {path}")

    return current_parent


# -------- Download --------
def download_from_drive(drive_path, local_path):
    """
    Downloads a file/folder from Google Drive (read-only).
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
        folder_id = get_folder_id(folder_path) if folder_path else DRIVE_ROOT

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
