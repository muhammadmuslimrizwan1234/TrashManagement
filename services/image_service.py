import os
import cv2
import numpy as np
from tempfile import NamedTemporaryFile

def dhash(image, hash_size=8):
    resized = cv2.resize(image, (hash_size + 1, hash_size))
    diff = resized[:, 1:] > resized[:, :-1]
    return sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])

def is_duplicate_image(file_storage, dataset_root):
    with NamedTemporaryFile(delete=False) as tmp:
        file_storage.save(tmp.name)
        img = cv2.imread(tmp.name, cv2.IMREAD_GRAYSCALE)
        new_hash = dhash(img)

    for root, _, files in os.walk(dataset_root):
        for f in files:
            existing_img = cv2.imread(os.path.join(root, f), cv2.IMREAD_GRAYSCALE)
            if existing_img is None:
                continue
            existing_hash = dhash(existing_img)
            if new_hash == existing_hash:
                return True
    return False
