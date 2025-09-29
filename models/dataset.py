import os
import shutil
from werkzeug.utils import secure_filename
from services.image_service import is_duplicate_image

DATASET_FOLDER = os.path.join(os.path.dirname(__file__), "..", "uploads", "dataset")
os.makedirs(DATASET_FOLDER, exist_ok=True)

def get_dataset_structure():
    """
    Returns nested category structure (Main -> Sub -> SubSub).
    """
    structure = {}
    for root, dirs, files in os.walk(DATASET_FOLDER):
        rel_path = os.path.relpath(root, DATASET_FOLDER)
        parts = rel_path.split(os.sep)
        current = structure
        for p in parts:
            if p == ".":
                continue
            current = current.setdefault(p, {})
        if files:
            current["_images"] = files
    return structure

def add_image_to_category(file, hierarchy):
    """
    Add image to given hierarchy (list of folders).
    """
    target_dir = os.path.join(DATASET_FOLDER, *hierarchy)
    os.makedirs(target_dir, exist_ok=True)

    filename = secure_filename(file.filename)
    target_path = os.path.join(target_dir, filename)

    # check duplicates across dataset
    if is_duplicate_image(file, DATASET_FOLDER):
        raise ValueError("Duplicate image already exists in dataset")

    file.save(target_path)
    return target_path

def delete_image_from_dataset(path):
    """
    Delete image from dataset by absolute path.
    """
    if os.path.exists(path):
        os.remove(path)
        return True
    return False
