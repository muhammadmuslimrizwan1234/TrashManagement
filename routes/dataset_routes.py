import os
from flask import Blueprint, request, jsonify
from models.dataset import get_dataset_structure, add_image_to_category, delete_image_from_dataset

dataset_bp = Blueprint("dataset", __name__)

@dataset_bp.route("/", methods=["GET"])
def get_dataset():
    return jsonify(get_dataset_structure()), 200

@dataset_bp.route("/add", methods=["POST"])
def add_image():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files["file"]
    hierarchy = request.form.get("hierarchy")
    if not hierarchy:
        return jsonify({"error": "Hierarchy required"}), 400

    hierarchy_list = hierarchy.split(">")
    try:
        path = add_image_to_category(file, hierarchy_list)
        return jsonify({"message": "Image added", "path": path}), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 409

@dataset_bp.route("/delete", methods=["DELETE"])
def delete_image():
    path = request.args.get("path")
    if not path:
        return jsonify({"error": "Path required"}), 400
    success = delete_image_from_dataset(path)
    if success:
        return jsonify({"message": "Image deleted"}), 200
    else:
        return jsonify({"error": "File not found"}), 404
