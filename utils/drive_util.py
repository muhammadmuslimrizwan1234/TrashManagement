from dotenv import load_dotenv
load_dotenv()

import os
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# -------- Setup --------
SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE = None

# Root folder (TrashAI-Dataset folder ID from .env)
DRIVE_ROOT = os.getenv("DRIVE_FOLDER_ID", "root")


def get_service():
    """Build Google Drive API service (cached)."""
    global SERVICE
    if SERVICE is None:
        service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT_JSON"))
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        SERVICE = build("drive", "v3", credentials=creds)
    return SERVICE


# -------- Helpers --------
def get_folder_id(folder_path, parent_id=DRIVE_ROOT):
    """
    Resolve a folder path like 'dataset/glass' step by step.
    """
    service = get_service()
    parts = folder_path.strip("/").split("/")

    current_parent = parent_id
    for part in parts:
        query = f"'{current_parent}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        items = results.get("files", [])

        # try to match ignoring case
        match = next((i for i in items if i["name"].lower() == part.lower()), None)
        if not match:
            # debug print
            print(f"📂 Available in '{current_parent}': {[i['name'] for i in items]}")
            raise FileNotFoundError(f"❌ '{folder_path}' folder not found inside root (ID={parent_id})")

        current_parent = match["id"]

    return current_parent

def resolve_dataset_path(drive_path):
    """
    Resolve inside the existing TrashAI-Dataset/dataset folder.
    Example:
      drive_path='dataset/Metal/Zinc/img.jpg'
      returns (dataset_folder_id, 'Metal/Zinc/img.jpg')
    """
    if not drive_path.startswith("dataset"):
        raise ValueError("All dataset paths must start with 'dataset/'")

    rel_path = drive_path[len("dataset/") :].strip("/")

    service = get_service()

    # 🔎 Always locate the already-existing "dataset" folder
    query = (
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='dataset' and '{DRIVE_ROOT}' in parents and trashed=false"
    )
    dataset_folders = (
        service.files()
        .list(q=query, fields="files(id, name)")
        .execute()
        .get("files", [])
    )

    if not dataset_folders:
        raise FileNotFoundError("❌ 'dataset' folder not found inside TrashAI-Dataset")

    dataset_id = dataset_folders[0]["id"]
    return dataset_id, rel_path


# -------- Download --------
def download_from_drive(drive_path, local_path):
    """
    Downloads a file/folder from Google Drive.
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


# -------- Upload --------
def upload_to_drive(local_path, drive_path):
    """
    Upload a local file into Google Drive dataset folder.
    Creates category/sub-category folders if missing.
    """
    service = get_service()
    dataset_folder_id, rel_path = resolve_dataset_path(drive_path)

    folder_path, filename = os.path.split(rel_path)

    # create only category/sub-category if not exist
    parent_id = get_folder_id(folder_path, parent_id=dataset_folder_id, create=True)

    file_metadata = {"name": filename, "parents": [parent_id]}
    media = MediaFileUpload(local_path, resumable=True)

    file = service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    print(f"⬆️ Uploaded {local_path} -> {drive_path} (id={file['id']})")
    return file["id"]

# -------- Delete --------
def delete_from_drive(drive_path):
    """
    Delete a file or folder from dataset.
    Only deletes the final target (not the whole parent chain).
    """
    service = get_service()
    dataset_folder_id, rel_path = resolve_dataset_path(drive_path)

    if not rel_path:
        print("⚠️ Refusing to delete root dataset folder.")
        return

    parent_path, target_name = os.path.split(rel_path)
    parent_id = get_folder_id(parent_path, parent_id=dataset_folder_id, create=False)

    query = f"name='{target_name}' and '{parent_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, mimeType)").execute()
    files = results.get("files", [])

    if not files:
        print(f"⚠️ Drive delete failed: {drive_path} not found")
        return

    for file in files:
        try:
            service.files().delete(fileId=file["id"]).execute()
            print(f"🗑️ Deleted {drive_path} ({file['mimeType']})")
        except Exception as e:
            print(f"⚠️ Failed to delete {drive_path}: {e}")


# -------- Move --------
def move_in_drive(old_drive_path, new_drive_path):
    """Move/rename a file or folder inside dataset."""
    service = get_service()

    old_dataset_id, old_rel_path = resolve_dataset_path(old_drive_path)
    new_dataset_id, new_rel_path = resolve_dataset_path(new_drive_path, create=False)

    old_parent, old_filename = os.path.split(old_rel_path)
    new_parent, new_filename = os.path.split(new_rel_path)

    old_parent_id = get_folder_id(old_parent, parent_id=old_dataset_id)
    new_parent_id = get_folder_id(new_parent, parent_id=new_dataset_id, create=True)

    query = f"name='{old_filename}' and '{old_parent_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if not files:
        print(f"⚠️ {old_drive_path} not found in Drive for move")
        return None

    file_id = files[0]["id"]
    file = service.files().get(fileId=file_id, fields="parents").execute()
    prev_parents = ",".join(file.get("parents", []))

    updated = service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=prev_parents,
        body={"name": new_filename},
        fields="id, parents"
    ).execute()

    print(f"📂 Moved {old_drive_path} -> {new_drive_path}")
    return updated["id"]
def debug_list_root():
    service = get_service()
    print(f"📂 Folders under DRIVE_ROOT (ID={DRIVE_ROOT}):")
    query = f"'{DRIVE_ROOT}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    items = results.get("files", [])
    for item in items:
        print(f" - {item['name']} ({item['mimeType']}) [{item['id']}]")
    if not items:
        print("⚠️ No items found under this root!")


