import cv2
import os
import imagehash
from PIL import Image

def calculate_image_hash(image):
    pil_image = Image.fromarray(image)
    return imagehash.average_hash(pil_image)

def is_duplicate(image, hash_set, threshold=5):
    image_hash = calculate_image_hash(image)
    for stored_hash in hash_set:
        if abs(stored_hash - image_hash) < threshold:
            return True
    return False

def extract_frames(video_path, output_dir):
    cap = cv2.VideoCapture(video_path)
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    hash_set = set()

    for i in range(frame_count):
        ret, frame = cap.read()
        if not ret:
            break

        # Check for duplicate frames
        if is_duplicate(frame, hash_set):
            continue

        # Save the frame as an image
        frame_path = os.path.join(output_dir, f"frame_{i:05d}.jpg")
        cv2.imwrite(frame_path, frame)

        # Calculate hash and store for duplicate checking
        image_hash = calculate_image_hash(frame)
        hash_set.add(image_hash)

    cap.release()


video_path = r'C:\Users\xufia\OneDrive\Imagens\Videos\_Videos finais - EN\Luxurious Rose Soap Recipe.mp4'
output_dir = r'C:\Users\xufia\OneDrive\Imagens\Videos\_Videos finais - EN\images'

extract_frames(video_path, output_dir)
