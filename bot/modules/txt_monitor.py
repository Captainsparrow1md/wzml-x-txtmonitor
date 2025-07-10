
import asyncio
import logging
import os
import re
import requests
import ffmpeg
from datetime import datetime
from pyrogram.errors import FloodWait, MessageNotModified
from bot.core.tg_client import TgClient
from bot.helper.telegram_helper.filters import CustomFilters
from pyrogram.handlers import MessageHandler
from pyrogram import filters

logger = logging.getLogger(__name__)
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

monitor_task = None
active_tasks = set()
created_topics = {}

TARGET_GROUP_ID = int(os.environ.get("TXT_TARGET_ID", "0"))

class Timer:
    def __init__(self, time_between=5):
        self.start_time = time.time()
        self.time_between = time_between

    def can_send(self):
        if time.time() > (self.start_time + self.time_between):
            self.start_time = time.time()
            return True
        return False

def hrb(value, digits=2):
    if value is None: return None
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0:
            return f"{value:.{digits}f}{unit}"
        value /= 1024.0
    return f"{value:.{digits}f}TiB"

def hrt(seconds, precision=0):
    pieces = []
    value = timedelta(seconds=seconds)
    if value.days: pieces.append(f"{value.days}d")
    seconds = value.seconds
    if seconds >= 3600:
        hours = seconds // 3600
        pieces.append(f"{hours}h")
        seconds -= hours * 3600
    if seconds >= 60:
        minutes = seconds // 60
        pieces.append(f"{minutes}m")
        seconds -= minutes * 60
    if seconds > 0 or not pieces:
        pieces.append(f"{seconds}s")
    return "".join(pieces[:precision or None])

timer = Timer()

async def progress_bar(current, total, reply, start):
    if timer.can_send():
        now = time.time()
        diff = now - start
        if diff < 1:
            return
        perc = f"{current * 100 / total:.1f}%"
        elapsed = round(diff)
        speed = current / elapsed
        eta = hrt((total - current) / speed if speed else 0, precision=1)
        sp = str(hrb(speed)) + "/s"
        tot = hrb(total)
        cur = hrb(current)
        bar = "▰" * int(current * 11 / total) + "▱" * (11 - int(current * 11 / total))
        try:
            await reply.edit(f"<b>╭──⬆️ Uploading
├⚡ {bar} | {perc}
├🚀 {sp}
├📟 {cur} of {tot} – ETA {eta}
╰────</b>")
        except FloodWait as e:
            await asyncio.sleep(e.value)

def parse_file_content(txt: str):
    lines = [ln.strip() for ln in txt.splitlines()]
    out, i = [], 0
    while i < len(lines):
        if not lines[i]:
            i += 1; continue
        if i + 1 < len(lines) and lines[i + 1].lower().startswith("http"):
            out.append((lines[i], lines[i + 1]))
            i += 2; continue
        http_idx = lines[i].lower().find("http")
        if http_idx != -1:
            out.append((lines[i][:http_idx].strip(), lines[i][http_idx:].strip()))
        i += 1
    return out

async def create_forum_topic(name: str) -> int:
    if name in created_topics:
        return created_topics[name]
    api = f"https://api.telegram.org/bot{TgClient.bot.token}/createForumTopic"
    res = await asyncio.to_thread(
        requests.post, api, data={"chat_id": TARGET_GROUP_ID, "name": name}
    )
    j = res.json()
    if not j.get("ok"):
        raise RuntimeError(j.get("description", "createForumTopic failed"))
    tid = j["result"]["message_thread_id"]
    created_topics[name] = tid
    return tid

async def process_file(txt_path: str):
    fname = os.path.basename(txt_path)
    topic_name = os.path.splitext(fname)[0]
    topic_id = await create_forum_topic(topic_name)
    status = await TgClient.bot.send_message(TARGET_GROUP_ID, f"📂 {fname}
⏳ preparing…", message_thread_id=topic_id)
    task = asyncio.current_task()
    active_tasks.add(task)
    try:
        entries = parse_file_content(open(txt_path, encoding="utf-8").read())
        total, sent = len(entries), 0
        await status.edit_text(f"📂 {fname}
⏳ 0/{total}")
        for title, url in entries:
            if task not in active_tasks: break
            await status.edit_text(f"📂 {fname}
🔄 {sent}/{total} – {title[:50]}")
            safe_title = re.sub(r"[\/*?:"<>|]", "_", title).strip()
            ext = ".pdf" if url.endswith(".pdf") else ".mp4"
            path = os.path.join(DOWNLOAD_DIR, f"{safe_title}{ext}")
            await download_and_upload(url, path, title, topic_id, status)
            sent += 1
            await status.edit_text(f"📂 {fname}
✅ {sent}/{total}")
        await status.edit_text(f"✅ {fname} – all {total} links sent")
    except Exception as e:
        await TgClient.bot.send_message(TARGET_GROUP_ID, f"❌ Error in {fname}: {e}", message_thread_id=topic_id)
    finally:
        active_tasks.discard(task)

async def download_and_upload(url, path, caption, topic_id, status_msg):
    with requests.get(url, stream=True, timeout=20) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        done = 0
        start = timer.start_time
        with open(path, "wb") as fh:
            for chunk in r.iter_content(1 << 14):
                fh.write(chunk)
                done += len(chunk)
                await progress_bar(done, total, status_msg, start)
    await TgClient.bot.send_document(TARGET_GROUP_ID, document=path, caption=caption, message_thread_id=topic_id)
    os.remove(path)

async def monitor_folder(folder="txt_files"):
    global monitor_task
    seen = set()
    await TgClient.bot.send_message(TARGET_GROUP_ID, f"📂 Monitoring `{folder}` for new txt files...")
    while True:
        try:
            current = {f for f in os.listdir(folder) if f.endswith(".txt")}
            new_files = sorted(current - seen)
            for fname in new_files:
                seen.add(fname)
                await process_file(os.path.join(folder, fname))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("monitor_folder error: %s", e)
        await asyncio.sleep(30)

async def stop_monitoring():
    global monitor_task
    if monitor_task:
        monitor_task.cancel()
        monitor_task = None
    active_tasks.clear()
    for f in os.listdir(DOWNLOAD_DIR):
        try: os.remove(os.path.join(DOWNLOAD_DIR, f))
        except: pass
    created_topics.clear()

async def start_txt_monitor(_, m):
    global monitor_task
    if monitor_task:
        await m.reply("Already running.")
        return
    monitor_task = asyncio.create_task(monitor_folder())
    await m.reply("Started monitoring.")

async def stop_txt_monitor(_, m):
    await stop_monitoring()
    await m.reply("Stopped and cleaned up.")

def add_txt_monitor_handlers():
    TgClient.bot.add_handler(
        MessageHandler(start_txt_monitor, filters.command("txtmonitor") & filters.private & CustomFilters.sudo)
    )
    TgClient.bot.add_handler(
        MessageHandler(stop_txt_monitor, filters.command("txtstop") & filters.private & CustomFilters.sudo)
    )
