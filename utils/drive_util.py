from dotenv import load_dotenv
load_dotenv()

import os
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# -------- Setup --------
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
SERVICE = None

# Root folder (shared drive folder ID instead of "root")
DRIVE_ROOT = os.getenv("DRIVE_FOLDER_ID", "root")


def get_service():
    global SERVICE
    if SERVICE is None:
        service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT_JSON"))
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        SERVICE = build("drive", "v3", credentials=creds)
    return SERVICE


# -------- Helpers --------
def get_folder_id(path, parent_id=None, create=False):
    """
    Get (or create if create=True) nested folders in Google Drive.
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
            if create:
                folder_metadata = {
                    "name": part,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [current_parent],
                }
                folder = service.files().create(body=folder_metadata, fields="id").execute()
                current_parent = folder["id"]
                print(f"📂 Created folder '{part}' in Drive")
            else:
                raise FileNotFoundError(f"Folder '{part}' not found in Drive path: {path}")

    return current_parent


# -------- Download --------
def download_from_drive(drive_path, local_path):
    service = get_service()

    # Folder case
    if not os.path.splitext(drive_path)[1]:
        folder_id = get_folder_id(drive_path, create=False)
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
        folder_path, filename = os.path.split(drive_path)
        folder_id = get_folder_id(folder_path, create=False) if folder_path else DRIVE_ROOT

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


# -------- Upload --------
def upload_to_drive(local_path, drive_path):
    """Upload local file into Google Drive dataset folder (auto-creates path)."""
    service = get_service()
    folder_path, filename = os.path.split(drive_path)
    folder_id = get_folder_id(folder_path, create=True)

    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print(f"⬆️ Uploaded {local_path} -> {drive_path} (id={file['id']})")
    return file["id"]


# -------- Delete --------
def delete_from_drive(drive_path):
    service = get_service()
    folder_path, filename = os.path.split(drive_path)
    folder_id = get_folder_id(folder_path, create=False)

    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])

    if not files:
        print(f"⚠️ {drive_path} not found in Drive for delete")
        return False

    file_id = files[0]["id"]
    service.files().delete(fileId=file_id).execute()
    print(f"🗑️ Deleted {drive_path} (id={file_id})")
    return True


def move_in_drive(old_drive_path, new_drive_path):
    service = get_service()

    # Find old file
    old_folder, old_filename = os.path.split(old_drive_path)
    old_folder_id = get_folder_id(old_folder, create=False)

    query = f"name='{old_filename}' and '{old_folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if not files:
        print(f"⚠️ {old_drive_path} not found in Drive for move")
        return None
    file_id = files[0]["id"]

    # Find/create new folder
    new_folder, new_filename = os.path.split(new_drive_path)
    new_folder_id = get_folder_id(new_folder, create=True)

    # Remove from old parent and add to new parent
    file = service.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file.get("parents", []))

    updated = service.files().update(
        fileId=file_id,
        addParents=new_folder_id,
        removeParents=prev_parents,
        body={"name": new_filename},
        fields="id, parents"
    ).execute()

    print(f"📂 Moved {old_drive_path} -> {new_drive_path}")
    return updated["id"]


def delete_folder_from_drive(drive_path):
    service = get_service()
    try:
        folder_id = get_folder_id(drive_path, create=False)
    except FileNotFoundError:
        print(f"⚠️ Folder {drive_path} not found in Drive")
        return False

    service.files().delete(fileId=folder_id).execute()
    print(f"🗑️ Deleted folder {drive_path} from Drive")
    return True
