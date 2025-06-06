#!/usr/bin/env python3

import argparse
import json
import logging
import time
from datetime import datetime

import cv2
from ultralytics import YOLO
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class ShelfStreamer:
    """
    Captures frames from a camera, performs YOLO detection, and streams
    detection events to a Kafka topic.
    """

    def __init__(
        self,
        model_path: str,
        kafka_server: str,
        kafka_topic: str,
        camera_source: str,
        conf_thres: float = 0.25,
        iou_thres: float = 0.45,
        device: str = "cpu",
    ):
        """
        Initialize the streamer with model and Kafka configuration.

        Args:
            model_path: path to YOLO .pt model file
            kafka_server: Kafka bootstrap server (host:port)
            kafka_topic: topic to publish detection events
            camera_source: camera index or video file path
            conf_thres: confidence threshold for detections
            iou_thres: IoU threshold for NMS
            device: compute device ('cpu' or GPU id)
        """
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.camera_source = camera_source

        logger.info("Loading YOLO model from %s", model_path)
        self.model = YOLO(model_path)
        self.model.conf = conf_thres
        self.model.iou = iou_thres

        logger.info("Connecting to Kafka at %s", kafka_server)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def detect_and_stream(self):
        """
        Continuously capture frames, detect objects, and send events to Kafka.
        """
        cap = cv2.VideoCapture(self.camera_source)
        if not cap.isOpened():
            logger.error("Failed to open camera/source: %s", self.camera_source)
            raise RuntimeError(f"Failed to open camera/source: {self.camera_source}")

        logger.info("Starting detection loop on source: %s", self.camera_source)
        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    logger.warning("Frame not read, retrying...")
                    time.sleep(0.1)
                    continue

                results = self.model(frame)
                detections = []
                for box in results[0].boxes:
                    cls_id = int(box.cls[0])
                    cls_name = results[0].names[cls_id]
                    conf = float(box.conf[0])

                    payload = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "class": cls_name,
                        "confidence": conf,
                        "source": str(self.camera_source),
                    }
                    detections.append(payload)
                    logger.debug("Publishing detection: %s", payload)

                for payload in detections:
                    self.producer.send(self.kafka_topic, payload)

                # Show annotated frame
                annotated = results[0].plot()
                cv2.imshow("Shelf Detection", annotated)
                if cv2.waitKey(1) & 0xFF == ord("q"):
                    logger.info("Detection loop interrupted by user.")
                    break

        finally:
            cap.release()
            cv2.destroyAllWindows()
            logger.info("Shut down camera and windows.")

    def run(self):
        """
        Entry point to start detection and streaming.
        """
        try:
            self.detect_and_stream()
        except KeyboardInterrupt:
            logger.info("Run stopped by user.")


def parse_args():
    parser = argparse.ArgumentParser(
        description="YOLO-based shelf detection and Kafka streaming"
    )
    parser.add_argument("--model", required=True, help="Path to YOLOv8 .pt model file")
    parser.add_argument(
        "--kafka-server", default="localhost:9092", help="Kafka bootstrap server"
    )
    parser.add_argument(
        "--kafka-topic",
        default="shelf_detections",
        help="Kafka topic for publishing detections",
    )
    parser.add_argument(
        "--camera-source", default=0, help="Camera index or path to video file"
    )
    parser.add_argument(
        "--conf-thres",
        type=float,
        default=0.25,
        help="Confidence threshold for detections",
    )
    parser.add_argument(
        "--iou-thres", type=float, default=0.45, help="IoU threshold for NMS"
    )
    parser.add_argument(
        "--device", default="cpu", help="Device for inference: cpu or GPU id"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    streamer = ShelfStreamer(
        model_path=args.model,
        kafka_server=args.kafka_server,
        kafka_topic=args.kafka_topic,
        camera_source=args.camera_source,
        conf_thres=args.conf_thres,
        iou_thres=args.iou_thres,
        device=args.device,
    )
    streamer.run()


if __name__ == "__main__":
    main()
