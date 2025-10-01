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


def resolve_dataset_path(drive_path):
    """
    Normalize drive_path so it's always relative to the existing 'dataset' folder.
    Returns (dataset_folder_id, relative_path).
    """
    service = get_service()
    if drive_path.startswith("dataset/"):
        drive_path = drive_path[len("dataset/"):]
    elif drive_path == "dataset":
        drive_path = ""

    dataset_folder_id = get_folder_id("dataset", create=False)
    return dataset_folder_id, drive_path


# -------- Upload --------
def upload_to_drive(local_path, drive_path):
    """
    Upload local file into Google Drive dataset folder (auto-creates path).
    """
    service = get_service()
    dataset_folder_id, rel_path = resolve_dataset_path(drive_path)

    folder_path, filename = os.path.split(rel_path)
    folder_id = get_folder_id(folder_path, parent_id=dataset_folder_id, create=True)

    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(local_path, resumable=True)

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print(f"⬆️ Uploaded {local_path} -> dataset/{rel_path} (id={file['id']})")
    return file["id"]


# -------- Delete --------
def delete_from_drive(drive_path):
    """
    Delete file from Google Drive (inside dataset).
    """
    service = get_service()
    dataset_folder_id, rel_path = resolve_dataset_path(drive_path)

    folder_path, filename = os.path.split(rel_path)
    folder_id = get_folder_id(folder_path, parent_id=dataset_folder_id, create=False)

    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])

    if not files:
        print(f"⚠️ {drive_path} not found in Drive for delete")
        return False

    file_id = files[0]["id"]
    service.files().delete(fileId=file_id).execute()
    print(f"🗑️ Deleted dataset/{rel_path} (id={file_id})")
    return True


# -------- Move --------
def move_in_drive(old_drive_path, new_drive_path):
    """
    Move (rename path) a file in Google Drive within dataset.
    """
    service = get_service()
    dataset_folder_id, old_rel = resolve_dataset_path(old_drive_path)
    _, new_rel = resolve_dataset_path(new_drive_path)

    old_folder, old_filename = os.path.split(old_rel)
    old_folder_id = get_folder_id(old_folder, parent_id=dataset_folder_id, create=False)

    query = f"name='{old_filename}' and '{old_folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if not files:
        print(f"⚠️ {old_drive_path} not found in Drive for move")
        return None
    file_id = files[0]["id"]

    # Find new folder (create if missing)
    new_folder, new_filename = os.path.split(new_rel)
    new_folder_id = get_folder_id(new_folder, parent_id=dataset_folder_id, create=True)

    # Move file
    file = service.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file.get("parents", []))

    updated = service.files().update(
        fileId=file_id,
        addParents=new_folder_id,
        removeParents=prev_parents,
        body={"name": new_filename},
        fields="id, parents"
    ).execute()

    print(f"📂 Moved dataset/{old_rel} -> dataset/{new_rel}")
    return updated["id"]
# -------- Delete Folder --------
def delete_folder_from_drive(drive_path):
    """
    Delete only the last folder in the given path from Google Drive (inside dataset).
    Example: delete_folder_from_drive("dataset/Metal/Zinc")
    """
    service = get_service()
    dataset_folder_id, rel_path = resolve_dataset_path(drive_path)

    if not rel_path.strip():
        print("⚠️ Refusing to delete root 'dataset' folder")
        return False

    parts = rel_path.strip("/").split("/")
    parent_parts = parts[:-1]
    folder_name = parts[-1]

    try:
        parent_id = get_folder_id("/".join(parent_parts), parent_id=dataset_folder_id, create=False)
    except FileNotFoundError:
        print(f"⚠️ Parent folder not found for path: {drive_path}")
        return False

    # Find the exact last folder (child of parent_id)
    query = (
        f"mimeType='application/vnd.google-apps.folder' "
        f"and name='{folder_name}' and '{parent_id}' in parents and trashed=false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        print(f"⚠️ Folder '{drive_path}' not found in Drive for delete")
        return False

    folder_id = files[0]["id"]

    try:
        service.files().delete(fileId=folder_id).execute()
        print(f"🗑️ Deleted folder {drive_path} (id={folder_id})")
        return True
    except Exception as e:
        print(f"⚠️ Drive delete folder failed: {e}")
        return False
