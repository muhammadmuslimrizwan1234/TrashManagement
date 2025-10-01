import os
import certifi
import urllib.parse
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from pymongo import MongoClient
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import traceback

# ---------------- Utils ----------------
from utils.file_utils import save_to_dataset, remove_duplicate_from_other_categories
from utils.category_utils import get_categories
from models.classifier import predict_image_file
from utils.drive_util import upload_to_drive, delete_from_drive, move_in_drive  # ✅ updated imports

# ---------------- Load Env ----------------
load_dotenv()

# ---------------- Config ----------------
ROOT = os.path.dirname(__file__)
UPLOAD_FOLDER = os.path.join(ROOT, "uploads")
DATASET_FOLDER = os.path.join(ROOT, "dataset")
MODEL_DIR = os.path.join(ROOT, "models")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DATASET_FOLDER, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "TrashApp")
PORT = int(os.getenv("PORT", 5000))
DEBUG = os.getenv("DEBUG", "True").lower() == "true"

# ---------------- Flask App ----------------
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
CORS(app)

# ---------------- MongoDB ----------------
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]

# ---------------- Health ----------------
@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

# ---------------- Predict ----------------
@app.route("/api/predict", methods=["POST"])
def predict():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    filename = secure_filename(file.filename)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    saved_path = os.path.join(app.config["UPLOAD_FOLDER"], f"{timestamp}_{filename}")
    file.save(saved_path)

    try:
        result = predict_image_file(saved_path)
        classification = result["objects"][0]  # first object
    except Exception as e:
        traceback.print_exc()
        print("🔥 PREDICT ERROR:", e)
        return jsonify({"error": str(e)}), 500

    label = classification["label"]
    confidence = float(classification["confidence"])
    if confidence < 0.6:
        label = "Unknown"

    hierarchy = classification.get("hierarchy", [])

    record = {
        "image_path": saved_path,
        "label": label,
        "hierarchy": hierarchy,
        "confidence": confidence,
        "dominant_color": classification.get("dominant_color", "#000000"),
        "timestamp": datetime.utcnow()
    }
    db["predictions"].insert_one(record)

    response = {
        "image_url": f"{request.host_url}uploads/{urllib.parse.quote(os.path.basename(saved_path))}",
        "label": record["label"],
        "hierarchy": hierarchy,
        "main_type": hierarchy[0] if len(hierarchy) > 0 else "N/A",
        "sub_type": hierarchy[1] if len(hierarchy) > 1 else "N/A",
        "sub_sub_type": hierarchy[2] if len(hierarchy) > 2 else "N/A",
        "confidence": round(record["confidence"] * 100, 2),
        "dominant_color": record["dominant_color"]
    }
    return jsonify(response), 201

# ---------------- Dataset Management ----------------
@app.route("/api/upload_dataset_image", methods=["POST"])
def upload_dataset_image():
    if "files" not in request.files and "file" not in request.files:
        return jsonify({"error": "No file(s) uploaded"}), 400

    files = request.files.getlist("files") if "files" in request.files else [request.files["file"]]

    hierarchy = {
        "main": request.form.get("main"),
        "sub": request.form.get("sub"),
        "subsub": request.form.get("subsub"),
    }

    if not hierarchy["main"]:
        return jsonify({"error": "Main category required"}), 400

    results = []
    for file in files:
        final_path, hash_value = save_to_dataset(file, hierarchy)
        remove_duplicate_from_other_categories(db, hash_value, final_path, hierarchy)

        rel_path = os.path.relpath(final_path, DATASET_FOLDER).replace(os.sep, "/")
        encoded_path = urllib.parse.quote(rel_path, safe="/")

        record = {
            "path": final_path,
            "rel_path": rel_path,
            "hierarchy": hierarchy,
            "hash": hash_value,
            "uploaded_by": request.form.get("user", "admin"),
            "timestamp": datetime.utcnow(),
        }
        db["dataset_images"].insert_one(record)

        # ✅ Upload to Drive
        drive_rel_path = f"dataset/{rel_path}"
        try:
            upload_to_drive(final_path, drive_rel_path)
        except Exception as e:
            print(f"⚠️ Drive upload failed: {e}")

        results.append({
            "message": "Image added",
            "path": final_path,
            "image_url": f"{request.host_url}dataset/{encoded_path}",
            "hierarchy": hierarchy,
        })

    return jsonify({"uploaded": len(results), "results": results}), 201


# ---------------- Categories ----------------
@app.route("/api/categories", methods=["GET"])
def categories():
    try:
        cats = get_categories()  # now fetched from Drive
        return jsonify(cats), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/dataset_images", methods=["GET"])
def list_dataset_images():
    images = list(db["dataset_images"].find({}, {"_id": 0}))
    for img in images:
        if "rel_path" in img:
            encoded_path = urllib.parse.quote(img["rel_path"], safe="/")
            img["image_url"] = f"{request.host_url}dataset/{encoded_path}"
    return jsonify({"count": len(images), "images": images}), 200


@app.route("/api/delete_category", methods=["POST"])
def delete_category():
    data = request.json
    main = data.get("main")
    sub = data.get("sub")
    subsub = data.get("subsub")

    if not main:
        return jsonify({"error": "main required"}), 400

    folder = os.path.join(DATASET_FOLDER, main)
    if sub:
        folder = os.path.join(folder, sub)
    if subsub:
        folder = os.path.join(folder, subsub)

    if os.path.exists(folder):
        import shutil
        shutil.rmtree(folder)

    # ✅ Delete from Drive as well (only last folder/file)
    drive_rel_path = "dataset/" + "/".join([p for p in [main, sub, subsub] if p])
    try:
        delete_from_drive(drive_rel_path)
    except Exception as e:
        print(f"⚠️ Drive folder delete failed: {e}")

    db["dataset_images"].delete_many({
        "hierarchy.main": main,
        "hierarchy.sub": sub,
        "hierarchy.subsub": subsub,
    })

    return jsonify({"message": "Category deleted"}), 200


@app.route("/api/delete_dataset_image/<hash_value>", methods=["DELETE"])
def delete_dataset_image(hash_value):
    doc = db["dataset_images"].find_one({"hash": hash_value})
    if not doc:
        return jsonify({"error": "Image not found"}), 404

    if os.path.exists(doc["path"]):
        os.remove(doc["path"])

    rel_path = doc.get("rel_path")
    if rel_path:
        try:
            drive_rel_path = f"dataset/{rel_path}"
            delete_from_drive(drive_rel_path)
        except Exception as e:
            print(f"⚠️ Drive delete failed: {e}")

    db["dataset_images"].delete_one({"hash": hash_value})
    return jsonify({"message": "Image deleted"}), 200

# ---------------- Serve uploaded images ----------------
@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)


@app.route("/dataset/<path:filename>")
def dataset_file(filename):
    return send_from_directory(DATASET_FOLDER, filename)

# ---------------- Main ----------------
if __name__ == "__main__":
    print("✅ Using local model and class names in /models/")
    app.run(host="0.0.0.0", port=PORT, debug=DEBUG)
