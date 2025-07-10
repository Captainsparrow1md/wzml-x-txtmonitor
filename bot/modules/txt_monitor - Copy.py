# ruff: noqa: F403, F405
from __future__ import annotations
import asyncio
import logging
import os
import re
import time
from asyncio import Queue
from typing import Dict, List, Tuple
import requests
import ffmpeg
from pyrogram import filters
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait, MessageNotModified
from bot.core.tg_client import TgClient
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.ext_utils.bot_utils import new_task
from bot.modules.mirror_leech import mirror_leech as upload_to_telegram  # WZML-X's uploader
from async_lru import alru_cache
from functools import lru_cache

# Configuration
DOWNLOAD_DIR = os.path.join(os.getenv("DOWNLOAD_DIR", "/usr/src/app/downloads"), "txt_files")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
MAX_FILE_SIZE_MB = 2000  # Telegram bot file size limit (2 GB)
MAX_RETRIES = 3  # Max retries for FloodWait
RETRY_BACKOFF = 5  # Base delay (seconds) between retries
TARGET_GROUP_ID = int(os.environ.get("TXT_TARGET_ID", "0"))  # Forum chat ID from env
INTERVAL = 30  # Folder check interval (seconds)

# Global state
monitor_task = None
active_tasks: set = set()  # Track active tasks for cleanup
created_topics: Dict[str, int] = {}  # Cache for forum topic IDs
logger = logging.getLogger(__name__)

# Utility classes/functions from VJ's utils.py
class Timer:
    def __init__(self):
        self.start_time = time.time()

    def elapsed(self, t2=None):
        return (t2 if t2 is not None else time.time()) - self.start_time

    def elapsed_text(self, t2=None):
        elapsed = self.elapsed(t2)
        return f"{int(elapsed // 60)}m {int(elapsed % 60)}s"

async def progress_bar(current, total, status_msg, start, scale=10):
    elapsed = Timer().elapsed(start)
    percent = (current / total) * 100 if total else 0
    bars = int(percent / scale)
    text = f"📤 {percent:.1f}% [{'█' * bars}{' ' * (scale - bars)}] {Timer().elapsed_text(start)}"
    try:
        await status_msg.edit_text(text)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.error(f"Progress update failed: {e}")

def hrb(n, suffix="B"):
    for unit in ["", "K", "M", "G", "T"]:
        if n < 1024:
            break
        n /= 1024
    return f"{n:.2f} {unit}{suffix}"

def hrt(seconds):
    if seconds < 0:
        seconds = 0
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:d}h {m:02d}m {s:02d}s" if h else f"{m:d}m {s:02d}s"

# Helper: Retry on FloodWait
async def retry_on_floodwait(coro, max_retries=MAX_RETRIES, backoff=RETRY_BACKOFF):
    for attempt in range(max_retries):
        try:
            return await coro
        except FloodWait as e:
            wait_time = e.value + (backoff * (2 ** attempt))
            logger.warning("FloodWait: Waiting %s seconds (attempt %d/%d)", wait_time, attempt + 1, max_retries)
            await asyncio.sleep(wait_time)
            if attempt == max_retries - 1:
                logger.error("Max retries reached for FloodWait")
                raise
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            raise

# Helper: Create forum topic (cached)
@alru_cache(maxsize=128)
async def create_forum_topic(client, name: str) -> int:
    if name in created_topics:
        return created_topics[name]
    api = f"https://api.telegram.org/bot{os.getenv('BOT_TOKEN')}/createForumTopic"
    async def send_request():
        data = await asyncio.to_thread(
            requests.post, api, data={"chat_id": TARGET_GROUP_ID, "name": name}
        )
        res = data.json()
        if not res.get("ok"):
            raise RuntimeError(res.get("description", "createForumTopic failed"))
        return res["result"]["message_thread_id"]
    
    tid = await retry_on_floodwait(send_request())
    created_topics[name] = tid
    logger.info("Created topic '%s' id=%s", name, tid)
    return tid

# Helper: Parse .txt (cached)
@lru_cache(maxsize=128)
def parse_file_content(txt: str) -> List[Tuple[str, str]]:
    lines = [ln.strip() for ln in txt.splitlines()]
    out, i = [], 0
    while i < len(lines):
        if not lines[i]:
            i += 1
            continue
        if i + 1 < len(lines) and lines[i + 1].lower().startswith("http"):
            out.append((lines[i], lines[i + 1]))
            i += 2
            continue
        http_idx = lines[i].lower().find("http")
        if http_idx != -1:
            out.append((lines[i][:http_idx].strip(), lines[i][http_idx:].strip()))
        i += 1
    return out

# Download helper
async def _download_with_progress(url: str, dest: str, status_msg):
    if os.path.exists(dest):
        os.remove(dest)
    logger.info("Starting download: %s to %s", url, dest)
    response = await asyncio.to_thread(requests.get, url, stream=True, timeout=20)
    response.raise_for_status()
    total = int(response.headers.get("content-length", 0))
    done = 0
    timer = Timer()
    with open(dest, "wb") as fh:
        for chunk in response.iter_content(1 << 14):
            fh.write(chunk)
            done += len(chunk)
            if total:
                await progress_bar(done, total, status_msg, timer.start_time)
    logger.info("Completed download: %s", dest)

# Upload helper (using WZML-X's uploader)
async def _upload_to_topic(client, path: str, caption: str, topic_id: int, status_msg):
    if not path or not isinstance(path, (str, bytes, os.PathLike)):
        logger.error("Invalid path: %s", path)
        await retry_on_floodwait(client.send_message(
            Config.OWNER_ID, f"⚠️ Upload failed: Invalid file path for {caption}"
        ))
        return

    logger.info("Starting upload: %s", path)
    if not os.path.exists(path):
        logger.error("File does not exist: %s", path)
        await retry_on_floodwait(client.send_message(
            Config.OWNER_ID, f"⚠️ Upload failed: File not found: {path}"
        ))
        return

    try:
        # Use WZML-X's upload_to_telegram for high-speed uploads
        task = await upload_to_telegram(
            client=client,
            message=status_msg,  # WZML-X expects a message object for status updates
            link=path,  # Treat file path as the link to upload
            is_file=True,  # Indicate it's a local file
            chat_id=TARGET_GROUP_ID,
            message_thread_id=topic_id,
            name=caption
        )
        logger.info("Completed upload: %s", path)
    except Exception as e:
        logger.error("Upload failed for %s: %s", path, e)
        await retry_on_floodwait(client.send_message(
            Config.OWNER_ID, f"⚠️ Upload failed for {os.path.basename(path)}: {e}"
        ))
    finally:
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError as e:
                logger.warning("Failed to delete file %s: %s", path, e)
        for f in os.listdir(DOWNLOAD_DIR):
            if f.endswith(".aria2"):
                try:
                    os.remove(os.path.join(DOWNLOAD_DIR, f))
                except OSError:
                    pass

# Per-.txt processor
@new_task
async def process_file(client, txt_path: str, addon: str):
    global active_tasks
    fname = os.path.basename(txt_path)
    topic_name = os.path.splitext(fname)[0]
    topic_id = await create_forum_topic(client, topic_name)
    status = await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"📂 {fname}\n⏳ preparing…"))
    task = asyncio.current_task()
    active_tasks.add(task)
    try:
        entries = parse_file_content(open(txt_path, encoding="utf-8").read())
        total = len(entries)
        sent = 0
        await retry_on_floodwait(status.edit_text(f"📂 {fname}\n⏳ 0/{total}"))

        upload_queue = Queue()

        async def download_and_enqueue(title: str, url: str):
            nonlocal sent
            if task not in active_tasks:
                return
            try:
                await retry_on_floodwait(status.edit_text(f"📂 {fname}\n🔄 {sent}/{total} – {title[:50]}"))
            except MessageNotModified:
                pass
            caption = f"{title}\n{addon}" if addon else title
            safe_title = re.sub(r"[\\/*?:\"<>|]", "_", title).strip()
            ext = ".pdf" if url.lower().endswith(".pdf") else ".mp4" if url.lower().endswith(".mp4") else ""
            path = os.path.join(DOWNLOAD_DIR, f"{safe_title}{ext}")
            try:
                await _download_with_progress(url, path, status)
                await upload_queue.put((path, caption))
            except Exception as e:
                logger.error("Download error on %s: %s", title, e)
                await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"⚠️ {title} failed\n`{e}`"))

        async def uploader():
            nonlocal sent
            while sent < total:
                try:
                    path, caption = await upload_queue.get()
                    await _upload_to_topic(client, path, caption, topic_id, status)
                    sent += 1
                    try:
                        await retry_on_floodwait(status.edit_text(f"📂 {fname}\n✅ {sent}/{total}"))
                    except MessageNotModified:
                        pass
                except Exception as e:
                    logger.error("Upload error: %s", e)
                    await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"⚠️ Upload error\n`{e}`"))
                finally:
                    upload_queue.task_done()

        uploader_task = asyncio.create_task(uploader())
        for title, url in entries:
            if task not in active_tasks:
                break
            await download_and_enqueue(title, url)

        await upload_queue.join()
        try:
            uploader_task.cancel()
        except asyncio.CancelledError:
            pass

        await retry_on_floodwait(status.edit_text(f"✅ {fname} – all {total} links sent"))
    except Exception as e:
        logger.error("Error processing %s: %s", fname, e)
        await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"❌ Error in {fname}: {e}"))
    finally:
        active_tasks.discard(task)

# Directory-wide processor
@new_task
async def process_all_files(client, directory: str, addon: str):
    global active_tasks
    if not os.path.isdir(directory):
        await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"❌ Dir not found: {directory}"))
        return
    files = [f for f in os.listdir(directory) if f.endswith(".txt")]
    if not files:
        await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"No .txt files in `{directory}`."))
        return
    for fname in sorted(files):
        txt_path = os.path.join(directory, fname)
        task = asyncio.create_task(process_file(client, txt_path, addon))
        active_tasks.add(task)
        logger.info("Processing %s", txt_path)
        await task
        active_tasks.discard(task)
        logger.info("Completed %s", txt_path)
    logger.info("All .txt files processed")

# Folder monitor
@new_task
async def monitor_folder(client, directory: str, interval: int = INTERVAL, addon: str = ""):
    global active_tasks
    seen = set()
    await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"📂 Monitoring `{directory}` (sequential txt processing)"))
    while True:
        try:
            current = {f for f in os.listdir(directory) if f.endswith(".txt")}
            new_files = sorted(current - seen)
            if new_files:
                for fname in new_files:
                    if fname not in seen:
                        txt_path = os.path.join(directory, fname)
                        seen.add(fname)
                        task = asyncio.create_task(process_file(client, txt_path, addon))
                        active_tasks.add(task)
                        logger.info("Processing new file %s", txt_path)
                        await task
                        active_tasks.discard(task)
                        logger.info("Completed %s", txt_path)
                logger.info("Processed batch of %d new files", len(new_files))
        except asyncio.CancelledError:
            logger.info("Monitor folder task cancelled")
            break
        except Exception as e:
            logger.exception("monitor_folder error: %s", e)
            await retry_on_floodwait(client.send_message(Config.OWNER_ID, f"❌ Monitor error: {e}"))
        await asyncio.sleep(interval)

# Command handlers
@new_task
async def cmd_txtmonitor_start(client, message):
    global monitor_task
    if monitor_task and not monitor_task.done():
        await message.reply("Text monitor already running!")
        return
    caption = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else ""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    monitor_task = asyncio.create_task(monitor_folder(client, DOWNLOAD_DIR, INTERVAL, caption))
    await message.reply("Started .txt file monitor.")

@new_task
async def cmd_txtmonitor_stop(client, message):
    global monitor_task, active_tasks
    if monitor_task:
        for task in active_tasks:
            task.cancel()
        active_tasks.clear()
        monitor_task.cancel()
        monitor_task = None
        await message.reply("Stopped .txt file monitor.")
    else:
        await message.reply("No monitor running.")

# Register handlers
def add_txt_monitor_handlers():
    TgClient.bot.add_handler(
        MessageHandler(
            cmd_txtmonitor_start,
            filters.command("txtmonitor") & filters.private & CustomFilters.sudo
        )
    )
    TgClient.bot.add_handler(
        MessageHandler(
            cmd_txtmonitor_stop,
            filters.command("txtstop") & filters.private & CustomFilters.sudo
        )
    )