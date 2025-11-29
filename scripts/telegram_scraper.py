#!/usr/bin/env python3
"""
telegram_scraper.py

Usage examples:
  python telegram_scraper.py --channels-file channels.txt --since 2025-05-01 --until 2025-06-01 --limit 500 --incremental
  python telegram_scraper.py --channels CheMed123 --limit 1000 --incremental
"""

import asyncio
import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone, date
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Set

from telethon import TelegramClient, errors
from telethon.tl.types import Message

# Optional: load .env if exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
API_ID = int(os.environ.get("TELEGRAM_API_ID", "0") or 0)
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
SESSION_NAME = os.environ.get("TELEGRAM_SESSION", "scraper.session")

BASE_DIR = PROJECT_ROOT / "data" / "raw"
MSG_DIR = BASE_DIR / "telegram_messages"
IMG_DIR = BASE_DIR / "images"
MANIFEST_DIR = MSG_DIR / "_manifests"
LOG_DIR = PROJECT_ROOT / "logs"

"""BASE_DIR = Path("data/raw")
MSG_DIR = BASE_DIR / "telegram_messages"
IMG_DIR = BASE_DIR / "images"
MANIFEST_DIR = MSG_DIR / "_manifests"
LOG_DIR = Path("./logs")"""
print("Directory scan completed")
for d in [MSG_DIR, IMG_DIR, MANIFEST_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Logging
logger = logging.getLogger("telegram_scraper")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
fh = RotatingFileHandler(LOG_DIR / "scraper.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
fh.setFormatter(fmt)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(fh)
logger.addHandler(ch)


def sanitize_filename(name: str) -> str:
    return re.sub(r"[^\w\-_\. ]+", "_", (name or "").strip())


def ensure_path(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def message_to_serializable(m: Message) -> dict:
    """Simplify Telethon message to JSON-serializable dict."""
    try:
        d = {
            "id": m.id,
            "peer_id": str(m.peer_id) if m.peer_id else None,
            "date": m.date.astimezone(timezone.utc).isoformat() if m.date else None,
            "sender_id": getattr(m, "from_id", None) and str(getattr(m.from_id, "user_id", getattr(m, "from_id"))),
            "text": m.message,
            "views": getattr(m, "views", None),
            "forwards": getattr(m, "forwards", None),
            "reply_to_msg_id": getattr(m, "reply_to_msg_id", None),
            "is_forward": getattr(m, "fwd_from", None) is not None,
            "media": None,
        }
        if m.media:
            if getattr(m, "photo", None):
                d["media"] = {"type": "photo"}
            elif getattr(m, "document", None):
                doc = m.document
                d["media"] = {
                    "type": "document",
                    "mime_type": getattr(doc, "mime_type", None),
                    "file_name": getattr(doc, "file_name", None),
                }
            else:
                d["media"] = {"type": type(m.media).__name__}
        d["_raw_repr"] = repr(m)[:500]
        return d
    except Exception as e:
        logger.exception("serialize error: %s", e)
        return {"id": getattr(m, "id", None), "error": f"serialize_failed:{e}"}


async def download_with_retries(client, message, dest_path: Path, max_retries: int = 4):
    """Download media with retries."""
    attempt = 0
    while attempt < max_retries:
        try:
            result = await client.download_media(message, file=str(dest_path))
            return Path(result) if result else None
        except errors.FloodWaitError as fw:
            wait = int(getattr(fw, "seconds", 60))
            logger.warning("FloodWait: sleeping %s seconds", wait)
            await asyncio.sleep(wait + 1)
        except Exception as e:
            attempt += 1
            wait = 2 ** attempt
            logger.warning("Download error attempt %d/%d: %s. retry in %ds", attempt, max_retries, e, wait)
            await asyncio.sleep(wait)
    logger.error("Download failed for msg %s", getattr(message, "id", None))
    return None


def load_seen_ids(channel: str) -> Set[int]:
    """Return set of already-saved message IDs for channel."""
    seen = set()
    sanitized = sanitize_filename(channel)
    for day_dir in MSG_DIR.iterdir():
        if not day_dir.is_dir():
            continue
        file_path = day_dir / f"{sanitized}.json"
        if not file_path.exists():
            continue
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        obj = json.loads(line)
                        if isinstance(obj.get("id"), int):
                            seen.add(obj["id"])
                    except Exception:
                        continue
        except Exception:
            continue
    return seen


async def scrape_channel(client: TelegramClient, channel: str, limit=None,
                         incremental=False, since: Optional[date] = None,
                         until: Optional[date] = None):
    sanitized = sanitize_filename(channel)
    ensure_path(IMG_DIR / sanitized)
    seen_ids = load_seen_ids(channel) if incremental else set()
    count = 0
    async for msg in client.iter_messages(channel, limit=limit):
        if msg is None or msg.id in seen_ids:
            continue

        msg_date = msg.date.date() if msg.date else date.today()
        # Apply date filters
        if since and msg_date < since:
            break
        if until and msg_date >= until:
            continue

        date_str = msg_date.isoformat()
        file_dir = MSG_DIR / date_str
        ensure_path(file_dir)
        file_path = file_dir / f"{sanitized}.json"

        serial = message_to_serializable(msg)
        with open(file_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(serial, ensure_ascii=False, default=str) + "\n")

        # Check if media is image
        is_image = False
        if getattr(msg, "photo", None):
            is_image = True
        elif getattr(msg, "document", None):
            mime = getattr(msg.document, "mime_type", "") or ""
            if mime.startswith("image"):
                is_image = True

        if is_image:
            ext = ".jpg"
            orig = getattr(getattr(msg, "document", None), "file_name", None)
            if orig and "." in orig:
                ext = Path(orig).suffix
            fname = f"{sanitized}_{msg.id}_{date_str}{ext}"
            dest = IMG_DIR / sanitized / fname
            i = 1
            while dest.exists():
                dest = IMG_DIR / sanitized / f"{Path(fname).stem}_{i}{ext}"
                i += 1
            downloaded = await download_with_retries(client, msg, dest)
            if downloaded:
                logger.info("Downloaded image: %s", downloaded)
            else:
                logger.warning("Failed to download image for msg %s", msg.id)

        count += 1
        if count % 50 == 0:
            await asyncio.sleep(0.3)

    logger.info("Finished %s (saved %d messages)", channel, count)
    return count


async def main(args):
    if not API_ID or not API_HASH:
        logger.error("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in environment")
        sys.exit(1)

    # Parse dates
    since_date = datetime.strptime(args.since, "%Y-%m-%d").date() if args.since else None
    until_date = datetime.strptime(args.until, "%Y-%m-%d").date() if args.until else None

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    logger.info("Connected to Telegram")

    # Read channels
    channels = []
    if args.channels_file and os.path.exists(args.channels_file):
        with open(args.channels_file, "r", encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    channels.append(ln)
    if args.channels:
        channels.extend(args.channels)
    channels = list(dict.fromkeys(channels))
    if not channels:
        logger.error("No channels provided")
        return

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    manifest = {
        "run_id": run_id,
        "channels": channels,
        "limit": args.limit,
        "incremental": args.incremental,
        "since": args.since,
        "until": args.until,
        "started_at": datetime.utcnow().isoformat() + "Z",
        "results": {},
    }

    for ch in channels:
        try:
            processed = await scrape_channel(
                client, ch, limit=args.limit, incremental=args.incremental,
                since=since_date, until=until_date
            )
            manifest["results"][ch] = {"processed": processed, "status": "ok"}
        except errors.FloodWaitError as fw:
            wait = int(getattr(fw, "seconds", 60)) + 1
            logger.warning("FloodWait: sleeping %s s", wait)
            await asyncio.sleep(wait)
            processed = await scrape_channel(
                client, ch, limit=args.limit, incremental=args.incremental,
                since=since_date, until=until_date
            )
            manifest["results"][ch] = {"processed": processed, "status": "ok_after_wait"}
        except Exception as e:
            logger.exception("Error scraping %s: %s", ch, e)
            manifest["results"][ch] = {"processed": 0, "status": f"error:{e}"}

    manifest["finished_at"] = datetime.utcnow().isoformat() + "Z"
    out = MANIFEST_DIR / f"{run_id}.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    logger.info("Run manifest saved: %s", out)

    await client.disconnect()
    logger.info("Disconnected")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--channels-file", type=str, default="channels.txt", help="File with channels (one per line)")
    parser.add_argument("--channels", nargs="+", help="Channels on command line")
    parser.add_argument("--limit", type=int, default=None, help="Max messages per channel")
    parser.add_argument("--incremental", action="store_true", help="Skip already-saved messages")
    parser.add_argument("--since", type=str, default=None, help="Start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--until", type=str, default=None, help="End date exclusive (YYYY-MM-DD)")
    args = parser.parse_args()
    asyncio.run(main(args))
