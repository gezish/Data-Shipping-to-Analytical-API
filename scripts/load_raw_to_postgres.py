#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import Json

def get_conn():
    import os
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST","localhost"),
        dbname=os.getenv("POSTGRES_DB","postgres"),
        user=os.getenv("POSTGRES_USER","postgres"),
        password=os.getenv("POSTGRES_PASSWORD","root"),
        port=int(os.getenv("POSTGRES_PORT","5432"))
    )

print("Db Connect succesfull")
def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            id serial PRIMARY KEY,
            channel text,
            message_id int,
            message_date timestamptz,
            raw jsonb
        );
        """)
        conn.commit()

def ingest_file(conn, channel, file_path):
    with open(file_path, "r", encoding="utf-8") as fh:
        rows = []
        for ln in fh:
            try:
                obj = json.loads(ln)
                message_id = obj.get("id")
                message_date = obj.get("date")
                rows.append((channel, message_id, message_date, Json(obj)))
            except Exception:
                continue
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO raw.telegram_messages (channel, message_id, message_date, raw) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", rows)
        conn.commit()
        return len(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/raw/telegram_messages")
    args = parser.parse_args()
    src = Path(args.source)
    conn = get_conn()
    ensure_table(conn)
    total = 0
    for day_dir in src.iterdir():
        if not day_dir.is_dir():
            continue
        for file in day_dir.glob("*.json"):
            channel_name = file.stem
            n = ingest_file(conn, channel_name, file)
            total += n
            print(f"Inserted {n} rows from {file}")
    print("Total inserted:", total)
    conn.close()

if __name__ == "__main__":
    main()
