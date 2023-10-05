import cv2
import pandas as pd
import os
import imagehash
from PIL import Image
import pytesseract
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def calculate_image_hash(image):
    pil_image = Image.fromarray(image)
    return imagehash.average_hash(pil_image)

def detect_text(image):
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, threshold = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    text = pytesseract.image_to_string(threshold)
    return text.strip()

def is_duplicate(image, hash_set, threshold=5):
    image_hash = calculate_image_hash(image)
    for stored_hash in hash_set:
        if abs(stored_hash - image_hash) < threshold:
            return True
    return False

def get_video_fps(video_path):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()
    return fps

def process_images(video_path):
    cap = cv2.VideoCapture(video_path)
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    hash_set = set()
    text_dict = {}
    text_list = []

    for i in range(frame_count):
        ret, frame = cap.read()
        if not ret:
            break

        # Check for duplicate frames
        if is_duplicate(frame, hash_set):
            continue

        # Detect text in the frame
        text = detect_text(frame)

        # Check if the image has text
        if not text:
            continue

        # Check if the text is not the same as in the previous image.
        # Only repeated text will be kept
#        if not (text in text_dict.values()):
#            continue

        # Save the frame as an image
        frame_path = os.path.join(output_dir, f"frame_{i:05d}.jpg")
        #cv2.imwrite(frame_path, frame)

        # Calculate hash and store for duplicate checking
        image_hash = calculate_image_hash(frame)
        hash_set.add(image_hash)

        # Store the image filename and detected text in a list of 
        # dictionaries
        frame_name = os.path.splitext(os.path.basename(frame_path))[0]
        fps = get_video_fps(video_path)
        time = int(frame_name.split('_')[1]) / int(fps)
        text_list.append (dict (image_name = frame_name,
                                secs = time, 
                                content = text))
        if len(text_list) > 2:
            if (str(text_list[1]['content']) != str(text_list[0]['content'])) and str(text_list[2]['content']) != str(text_list[1]['content']): 
                del text_list[0] 

        df_partial = pd.DataFrame (text_list)
        df_partial.to_csv (f'{output_dir}\contents.csv', index=False)

    cap.release()
    return text_list


video_path = r'C:\Users\xufia\OneDrive\Imagens\Videos\_Videos finais - EN\Luxurious Rose Soap Recipe.mp4'
output_dir = r'C:\Users\xufia\OneDrive\Imagens\Videos\_Videos finais - EN\images'

contents = process_images(video_path)
df = pd.DataFrame (contents)
print (df)
df.to_csv (f'{output_dir}\contents.csv')