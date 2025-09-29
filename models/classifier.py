# models/classifier.py
import cv2
import numpy as np
from sklearn.cluster import KMeans
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image as keras_image
import os
import json

# Load your trained model
clf_model = load_model(os.path.join("models", "model.h5"))

# Load class names mapping if you saved one during training
CLASS_NAMES_PATH = os.path.join("models", "class_names.json")
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
    return "#{:02x}{:02x}{:02x}".format(int(dominant_color[0]), int(dominant_color[1]), int(dominant_color[2]))

# ---------------- Classification ----------------
def classify_image(image_path):
    img = keras_image.load_img(image_path, target_size=(224, 224))
    img_array = keras_image.img_to_array(img) / 255.0
    img_array = np.expand_dims(img_array, axis=0)
    preds = clf_model.predict(img_array)
    class_idx = np.argmax(preds)
    confidence = float(preds[0][class_idx])

    if class_names:
        hierarchy = class_names[class_idx].split("_")  # recover hierarchy
    else:
        hierarchy = ["unknown"]

    return {
        "label": hierarchy[-1],
        "hierarchy": hierarchy,
        "confidence": confidence,
        "dominant_color": get_dominant_color(image_path)
    }

# ---------------- Single Image Prediction ----------------
def predict_image_file(image_path):
    classification = classify_image(image_path)
    return {"objects": [classification]}
