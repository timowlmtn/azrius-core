from kafka import KafkaConsumer
import json
from datetime import datetime
import time
from collections import defaultdict

# ─── CONFIG ─────────────────────────────────────────────────────
KAFKA_TOPIC = "shelf_detections"
KAFKA_SERVER = "localhost:9092"

inventory = defaultdict(lambda: {"last_seen": None, "count": 0, "source": None})

# ─── CONSUMER SETUP ─────────────────────────────────────────────
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

# ─── MAIN LOOP ───────────────────────────────────────────────────
print("[INFO] Starting inventory tracking consumer...")

try:
    for msg in consumer:
        data = msg.value
        item = data["class"]
        source = data.get("source", "unknown")
        timestamp = data.get("timestamp", datetime.utcnow().isoformat())

        state = inventory[item]
        state["last_seen"] = timestamp
        state["source"] = source
        state["count"] += 1  # You could also update a rolling window, etc.

        print(
            f"[UPDATE] {item}: seen at {timestamp} on {source}, count={state['count']}"
        )

except KeyboardInterrupt:
    print("\n[INFO] Consumer stopped. Final inventory state:")
    print(json.dumps(inventory, indent=2))
