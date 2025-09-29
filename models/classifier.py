# models/classifier.py
import cv2
import numpy as np
from sklearn.cluster import KMeans
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image as keras_image
import os, json, requests

# ---------------- Paths ----------------
MODEL_DIR = "models"
MODEL_PATH = os.path.join(MODEL_DIR, "model.h5")
CLASS_NAMES_PATH = os.path.join(MODEL_DIR, "class_names.json")

# ---------------- Google Drive File IDs ----------------
MODEL_FILE_ID = os.getenv("DRIVE_MODEL_ID", "")
CLASS_FILE_ID = os.getenv("DRIVE_CLASSES_ID", "")

# ---------------- Google Drive Downloader ----------------
def download_file_from_google_drive(file_id, dest_path):
    """Download a file from Google Drive by ID."""
    if not file_id:
        raise ValueError(f"Missing Google Drive file ID for {dest_path}")
    URL = "https://drive.google.com/uc?export=download"
    session = requests.Session()
    response = session.get(URL, params={"id": file_id}, stream=True)
    token = None
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            token = value
    if token:
        response = session.get(URL, params={"id": file_id, "confirm": token}, stream=True)
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(32768):
            if chunk:
                f.write(chunk)

# ---------------- Ensure Model Exists ----------------
os.makedirs(MODEL_DIR, exist_ok=True)

if not os.path.exists(MODEL_PATH):
    print("Downloading model.h5 from Google Drive...")
    download_file_from_google_drive(MODEL_FILE_ID, MODEL_PATH)

if not os.path.exists(CLASS_NAMES_PATH):
    print("Downloading class_names.json from Google Drive...")
    download_file_from_google_drive(CLASS_FILE_ID, CLASS_NAMES_PATH)

# ---------------- Load Model & Classes ----------------
clf_model = load_model(MODEL_PATH)

if os.path.exists(CLASS_NAMES_PATH):
    with open(CLASS_NAMES_PATH, "r") as f:
        class_names = json.load(f)
else:
    class_names = None

# ---------------- Dominant Color ----------------
def get_dominant_color(img_path, k=3):
    img = cv2.imread(img_path)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    img = img.reshape((-1, 3))
    kmeans = KMeans(n_clusters=k, random_state=0).fit(img)
    counts = np.bincount(kmeans.labels_)
    dominant_color = kmeans.cluster_centers_[np.argmax(counts)]
    return "#{:02x}{:02x}{:02x}".format(
        int(dominant_color[0]), int(dominant_color[1]), int(dominant_color[2])
    )

# ---------------- Classification ----------------
def classify_image(image_path):
    img = keras_image.load_img(image_path, target_size=(224, 224))
    img_array = keras_image.img_to_array(img) / 255.0
    img_array = np.expand_dims(img_array, axis=0)
    preds = clf_model.predict(img_array)
    class_idx = np.argmax(preds)
    confidence = float(preds[0][class_idx])

    if class_names:
        hierarchy = class_names[class_idx].split("_")
    else:
        hierarchy = ["unknown"]

    return {
        "label": hierarchy[-1],
        "hierarchy": hierarchy,
        "confidence": confidence,
        "dominant_color": get_dominant_color(image_path),
    }

# ---------------- Single Image Prediction ----------------
def predict_image_file(image_path):
    classification = classify_image(image_path)
    return {"objects": [classification]}
