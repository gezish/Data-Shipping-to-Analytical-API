import os
import json
from pathlib import Path
import pandas as pd
from ultralytics import YOLO
import psycopg2
from psycopg2.extras import Json
import re
import cv2

# ---------------------------------------------------------
# PROJECT ROOT RESOLUTION
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Directories
IMG_DIR = PROJECT_ROOT / "data" / "raw" / "images"
OUT_DIR = PROJECT_ROOT / "data" / "raw" / "yolo_outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# YOLO model
model = YOLO("yolov8n.pt")

# Postgres connection
POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "root"),
}

# ---------------------------------------------------------
# IMAGE VALIDATION
# ---------------------------------------------------------
def is_valid_image(image_path: Path) -> bool:
    """Return True only if OpenCV can load the image."""
    if not image_path.exists():
        return False
    if image_path.stat().st_size < 500:  # Under 500 bytes = corrupted
        return False
    try:
        img = cv2.imread(str(image_path))
        if img is None:
            return False
    except Exception:
        return False
    return True

# ---------------------------------------------------------
# DB connection
# ---------------------------------------------------------
def get_conn():
    return psycopg2.connect(**POSTGRES)

# ---------------------------------------------------------
# Ensure table exists
# ---------------------------------------------------------
def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.image_detections (
            id SERIAL PRIMARY KEY,
            channel TEXT,
            message_id INT,
            image_path TEXT,
            detection JSONB
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_image_unique
          ON raw.image_detections(channel, message_id, image_path);
        """)
        conn.commit()

# ---------------------------------------------------------
# YOLO inference
# ---------------------------------------------------------
def run_yolo_on_image(image_path: Path):
    try:
        results = model.predict(
            source=str(image_path),
            imgsz=640,
            conf=0.35,
            save=False,
            verbose=False
        )

        detections = []
        for r in results:
            for box in r.boxes:
                detections.append({
                    "class_id": int(box.cls.cpu().numpy()[0]),
                    "confidence": float(box.conf.cpu().numpy()[0]),
                    "xyxy": [float(x) for x in box.xyxy.cpu().numpy()[0]],
                    "class_name": model.names[int(box.cls)],
                })
        return detections

    except Exception as e:
        print(f"[YOLO ERROR] Cannot process image: {image_path} → {e}")
        return []

# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def main():
    all_records = []
    conn = get_conn()
    ensure_table(conn)

    for channel_dir in IMG_DIR.iterdir():
        if not channel_dir.is_dir():
            continue

        print(f"\nProcessing channel: {channel_dir.name}")

        for img_path in channel_dir.glob("*"):

            # Skip invalid or corrupted images
            if not is_valid_image(img_path):
                print(f"Skipping invalid image: {img_path}")
                continue

            detections = run_yolo_on_image(img_path)
            if not detections:
                continue

            # Extract numeric message ID (first number found in filename)
            m = re.search(r"(\d+)", img_path.stem)
            message_id = int(m.group(1)) if m else None

            record = {
                "channel": channel_dir.name,
                "message_id": message_id,
                "image_path": str(img_path),
                "detection": detections
            }
            all_records.append(record)

    # Save JSON output
    with open(OUT_DIR / "all_detections.json", "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    # Insert into Postgres
    with conn.cursor() as cur:
        for rec in all_records:
            cur.execute("""
                INSERT INTO raw.image_detections (channel, message_id, image_path, detection)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (channel, message_id, image_path) DO NOTHING;
            """, (rec["channel"], rec["message_id"], rec["image_path"], Json(rec["detection"])))
        conn.commit()

    print(f"\nSaved {len(all_records)} YOLO detections.")
    conn.close()


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------
if __name__ == "__main__":
    main()
