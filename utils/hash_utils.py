import imagehash
from PIL import Image

def get_image_hash(image_path):
    img = Image.open(image_path)
    return str(imagehash.average_hash(img))
