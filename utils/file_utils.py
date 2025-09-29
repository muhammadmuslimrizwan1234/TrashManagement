import os
import shutil
from werkzeug.utils import secure_filename
from utils.hash_utils import get_image_hash

# dataset root folder
DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "dataset")


def ensure_folder(path):
    """Create folder if it doesn’t exist."""
    os.makedirs(path, exist_ok=True)


def get_unique_filename(directory, filename):
    """
    Ensure filename is unique inside directory.
    If file exists, append _1, _2, etc. before extension.
    """
    base, ext = os.path.splitext(filename)
    counter = 1
    new_filename = filename
    while os.path.exists(os.path.join(directory, new_filename)):
        new_filename = f"{base}_{counter}{ext}"
        counter += 1
    return new_filename


def save_to_dataset(file, hierarchy):
    """
    Save uploaded image into dataset structure with unique filename.
    Returns (final_path, hash_value).
    """
    # Build dataset/<main>/<sub>/<subsub>/
    path_parts = [DATASET_DIR, hierarchy["main"]]
    if hierarchy.get("sub"):
        path_parts.append(hierarchy["sub"])
    if hierarchy.get("subsub"):
        path_parts.append(hierarchy["subsub"])
    final_dir = os.path.join(*path_parts)
    ensure_folder(final_dir)

    filename = secure_filename(file.filename)
    filename = get_unique_filename(final_dir, filename)  # ✅ avoid overwrite
    final_path = os.path.join(final_dir, filename)

    file.save(final_path)
    return final_path, get_image_hash(final_path)


def move_existing_file(existing_doc, new_hierarchy):
    """
    Move an already-existing image into a new category folder
    with a unique filename. Returns (new_path, rel_path).
    """
    old_path = existing_doc["path"]

    # Build new folder path
    path_parts = [DATASET_DIR, new_hierarchy["main"]]
    if new_hierarchy.get("sub"):
        path_parts.append(new_hierarchy["sub"])
    if new_hierarchy.get("subsub"):
        path_parts.append(new_hierarchy["subsub"])
    final_dir = os.path.join(*path_parts)
    os.makedirs(final_dir, exist_ok=True)

    # Generate unique filename in target folder
    filename = os.path.basename(old_path)
    filename = get_unique_filename(final_dir, filename)

    new_path = os.path.join(final_dir, filename)
    shutil.move(old_path, new_path)

    rel_path = os.path.relpath(new_path, DATASET_DIR).replace(os.sep, "/")
    return new_path, rel_path


def remove_duplicate_from_other_categories(db, hash_value, final_path, hierarchy):
    """
    If duplicate image exists in another category:
      - Move it into the new category with unique filename
      - Update DB record
    """
    dataset_images = db["dataset_images"]

    existing_doc = dataset_images.find_one({"hash": hash_value})
    if existing_doc:
        old_hierarchy = existing_doc["hierarchy"]

        # same category → do nothing
        if old_hierarchy == hierarchy:
            return

        # different category → move file + update DB
        new_path, rel_path = move_existing_file(existing_doc, hierarchy)
        dataset_images.update_one(
            {"_id": existing_doc["_id"]},
            {"$set": {
                "path": new_path,
                "rel_path": rel_path,
                "hierarchy": hierarchy
            }}
        )
