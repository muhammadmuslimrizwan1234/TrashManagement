import os

DATASET_DIR = os.path.join(os.path.dirname(__file__), "..", "dataset")

def get_categories():
    """
    Scan dataset/ folder and return hierarchy structure
    """
    def scan_dir(path):
        out = {}
        for entry in os.listdir(path):
            entry_path = os.path.join(path, entry)
            if os.path.isdir(entry_path):
                out[entry] = scan_dir(entry_path)
        return out

    return scan_dir(DATASET_DIR)
