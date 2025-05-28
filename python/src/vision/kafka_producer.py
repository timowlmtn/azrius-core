#!/usr/bin/env python3

import cv2
import json
import time
from datetime import datetime
from ultralytics import YOLO
from kafka import KafkaProducer

# ─── CONFIGURATION ───────────────────────────────────────────────
KAFKA_TOPIC = "shelf_detections"
KAFKA_SERVER = "localhost:9092"
CAMERA_SOURCE = 0  # Use 0 for webcam or path to video file

# ─── INIT MODELS AND KAFKA PRODUCER ──────────────────────────────
print("[INFO] Loading YOLO model...")
model = YOLO("yolov8n.pt")  # or path to your fine-tuned model

print("[INFO] Connecting to Kafka...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# ─── CAPTURE AND STREAM LOOP ─────────────────────────────────────
def detect_and_stream():
    cap = cv2.VideoCapture(CAMERA_SOURCE)
    if not cap.isOpened():
        raise RuntimeError("Failed to open camera/video source.")

    print("[INFO] Starting detection loop...")
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                print("[WARN] Frame not read, retrying...")
                time.sleep(0.1)
                continue

            results = model(frame)
            for box in results[0].boxes:
                cls_id = int(box.cls[0])
                cls_name = results[0].names[cls_id]
                conf = float(box.conf[0])

                payload = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "class": cls_name,
                    "confidence": conf,
                    "source": "camera_0",
                }

                print(f"[STREAM] {payload}")
                producer.send(KAFKA_TOPIC, payload)

            # Optional: show annotated frame
            annotated = results[0].plot()
            cv2.imshow("Shelf Detection", annotated)

            if cv2.waitKey(1) & 0xFF == ord("q"):
                print("[INFO] Exiting loop.")
                break

    finally:
        cap.release()
        cv2.destroyAllWindows()


# ─── MAIN ────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        detect_and_stream()
    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
