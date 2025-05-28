from ultralytics import YOLO
import cv2
import matplotlib.pyplot as plt

# Load model and image
model = YOLO("yolov8n.pt")
results = model("../../../data/vision/jw_oos_sm.png")

# Get OpenCV image with bounding boxes
annotated = results[0].plot()  # returns NumPy array (BGR)

# Convert BGR to RGB for matplotlib
rgb_annotated = cv2.cvtColor(annotated, cv2.COLOR_BGR2RGB)

cv2.imwrite("../../../data/vision/output.jpg", annotated)  # Saves with boxes
