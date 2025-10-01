import os
import json
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

# -------- Drive Setup --------
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
SERVICE = None
DRIVE_ROOT = os.getenv("DRIVE_FOLDER_ID", "root")  # shared drive root folder ID

def get_service():
    global SERVICE
    if SERVICE is None:
        service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT_JSON"))
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES
        )
        SERVICE = build("drive", "v3", credentials=creds)
    return SERVICE


def get_categories():
    """
    Fetch dataset folder structure (hierarchy) from Google Drive only.
    Example return:
    {
        "Plastic": {
            "Bottle": {
                "Coke": {}
            }
        },
        "Metal": {}
    }
    """
    service = get_service()

    def scan_folder(folder_id):
        result = {}
        query = (
            f"'{folder_id}' in parents and "
            f"mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        folders = (
            service.files()
            .list(q=query, fields="files(id, name)")
            .execute()
            .get("files", [])
        )
        for f in folders:
            result[f["name"]] = scan_folder(f["id"])
        return result

    # Look for "dataset" folder inside root
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
        return {}

    dataset_id = dataset_folders[0]["id"]
    return scan_folder(dataset_id)
