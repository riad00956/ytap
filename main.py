import os
import sys
import json
import asyncio
import logging
import threading
import uuid
import io
import shutil
import time
from contextlib import asynccontextmanager

# 1. Nest Asyncio সেটআপ (Render এর জন্য জরুরি)
import nest_asyncio
nest_asyncio.apply()

# FastAPI imports
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Telegram imports
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# Other imports
import yt_dlp
import aiohttp

# Matplotlib সেটআপ (Error Handling সহ)
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    print("WARNING: Matplotlib not available, graphs will be disabled.")
    MATPLOTLIB_AVAILABLE = False

# ============================================================================
# CONFIGURATION
# ============================================================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# টোকেন চেক
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN environment variable is missing!")

DOWNLOAD_DIR = "downloads"
# FFmpeg পাথ সেট করা
BIN_PATH = os.path.join(os.getcwd(), "bin")
os.environ["PATH"] += os.pathsep + BIN_PATH

# ডাউনলোড ফোল্ডার রিসেট
if os.path.exists(DOWNLOAD_DIR):
    try:
        shutil.rmtree(DOWNLOAD_DIR)
    except Exception as e:
        logger.warning(f"Could not clean download dir: {e}")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ============================================================================
# GLOBAL STATE
# ============================================================================
download_tasks = {}
active_downloads = {}
ptb_application = None

# ============================================================================
# HELPERS
# ============================================================================
class DownloadRequest(BaseModel):
    url: str
    format_id: str
    media_type: str

class ProgressTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.percent = 0.0
        self.speed = 0.0
        self.status = "downloading"
        self.filename = None
        self.error = None
        self.speed_history = []
        self.start_time = time.time()

    def update(self, d):
        with self.lock:
            if d["status"] == "downloading":
                self.status = "downloading"
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes", 0)
                
                if total:
                    self.percent = (downloaded / total) * 100
                
                self.speed = d.get("speed", 0)
                if self.speed:
                    elapsed = time.time() - self.start_time
                    self.speed_history.append((elapsed, self.speed))
                    
            elif d["status"] == "finished":
                self.status = "finished"
                self.filename = d.get("filename")
            elif d["status"] == "error":
                self.status = "error"
                self.error = d.get("error", "Unknown error")

    def snapshot(self):
        with self.lock:
            return {
                "percent": self.percent,
                "speed": self.speed,
                "status": self.status,
                "filename": self.filename,
                "error": self.error,
                "speed_history": list(self.speed_history)
            }

def download_worker(task_id: str, url: str, format_id: str, media_type: str):
    tracker = ProgressTracker()
    download_tasks[task_id]["tracker"] = tracker

    def progress_hook(d):
        tracker.update(d)

    output_template = os.path.join(DOWNLOAD_DIR, f"{task_id}_%(title)s.%(ext)s")
    
    ydl_opts = {
        "format": format_id,
        "outtmpl": output_template,
        "progress_hooks": [progress_hook],
        "quiet": True,
        "no_warnings": True,
        "ffmpeg_location": BIN_PATH,
    }

    if media_type == "audio":
        ydl_opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
    except Exception as e:
        logger.error(f"Download failed: {e}")
        tracker.update({"status": "error", "error": str(e)})
    finally:
        download_tasks[task_id]["done"] = True

def generate_speed_graph(speed_history):
    if not MATPLOTLIB_AVAILABLE or len(speed_history) < 2:
        return None
    try:
        times, speeds = zip(*speed_history)
        speeds_mbps = [s / 1_000_000 for s in speeds]
        
        plt.figure(figsize=(10, 5))
        plt.plot(times, speeds_mbps, color='#00ff00', linewidth=1.5)
        plt.fill_between(times, speeds_mbps, alpha=0.3, color='#00ff00')
        plt.title("Download Speed (Mbps)", color='white')
        plt.grid(True, alpha=0.2)
        plt.gca().set_facecolor('#1a1a1a')
        plt.gcf().patch.set_facecolor('#2a2a2a')
        plt.tick_params(colors='white')
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', facecolor='#2a2a2a', bbox_inches='tight')
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Graph error: {e}")
        return None

# ============================================================================
# BOT LOGIC
# ============================================================================
async def get_formats(url: str, filter_type: str):
    loop = asyncio.get_running_loop()
    def _fetch():
        try:
            with yt_dlp.YoutubeDL({"quiet": True}) as ydl:
                info = ydl.extract_info(url, download=False)
                formats = info.get("formats", [])
                if filter_type == "audio":
                    return [f for f in formats if f.get("vcodec") == "none"]
                return [f for f in formats if f.get("vcodec") != "none"]
        except Exception:
            return []
    return await loop.run_in_executor(None, _fetch)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 **Ready to download!** Send a YouTube link.")

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if "youtu" in text:
        active_downloads[update.effective_chat.id] = {"url": text}
        keyboard = [
            [InlineKeyboardButton("🎵 Audio", callback_data="audio"),
             InlineKeyboardButton("🎬 Video", callback_data="video")]
        ]
        await update.message.reply_text("Select format:", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("❌ Send a valid YouTube link.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    data = query.data

    if data in ["audio", "video"]:
        if chat_id not in active_downloads:
            await query.edit_message_text("⚠️ Session expired.")
            return
        
        await query.edit_message_text(f"🔍 Fetching {data} formats...")
        formats = await get_formats(active_downloads[chat_id]["url"], data)
        
        if not formats:
            await query.edit_message_text("❌ No formats found.")
            return

        keyboard = []
        seen = set()
        count = 0
        formats.sort(key=lambda x: x.get('filesize') or 0, reverse=True)

        for f in formats:
            fid = f['format_id']
            if fid in seen: continue
            
            label = f"{int(f.get('abr', 0))} kbps" if data == "audio" else f"{f.get('height')}p"
            if label != "0 kbps" and label != "Nonep":
                seen.add(fid)
                keyboard.append([InlineKeyboardButton(label, callback_data=f"dl_{data}_{fid}")])
                count += 1
            if count >= 5: break
            
        await query.edit_message_text("Select Quality:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("dl_"):
        _, type_, fid = data.split("_", 2)
        url = active_downloads[chat_id]["url"]
        task_id = str(uuid.uuid4())
        
        download_tasks[task_id] = {
            "req": {"url": url}, "done": False, "tracker": None
        }
        
        threading.Thread(target=download_worker, args=(task_id, url, fid, type_), daemon=True).start()
        await query.edit_message_text("🚀 Downloading...")
        await monitor_download(chat_id, query.message.id, task_id, context, type_)

async def monitor_download(chat_id, msg_id, task_id, context, media_type):
    last_text = ""
    start_ts = time.time()
    
    while True:
        task = download_tasks.get(task_id)
        if not task: break
        
        tracker = task.get("tracker")
        if tracker:
            snap = tracker.snapshot()
            if snap["status"] in ["finished", "error"]: break
            
            if time.time() - start_ts > 3:
                text = f"📥 **Downloading...** {snap['percent']:.1f}%"
                if text != last_text:
                    try:
                        await context.bot.edit_message_text(chat_id, msg_id, text=text, parse_mode='Markdown')
                        last_text = text
                        start_ts = time.time()
                    except: pass
        await asyncio.sleep(1)

    task = download_tasks.get(task_id)
    if task and task.get("done"):
        snap = task["tracker"].snapshot()
        if snap["status"] == "finished" and snap["filename"] and os.path.exists(snap["filename"]):
            fpath = snap["filename"]
            # Rename if needed for audio
            if media_type == "audio" and not fpath.endswith(".mp3"):
                base = os.path.splitext(fpath)[0]
                if os.path.exists(base + ".mp3"): fpath = base + ".mp3"

            try:
                await context.bot.edit_message_text(chat_id, msg_id, text="📤 Uploading...")
                graph = generate_speed_graph(snap["speed_history"])
                
                with open(fpath, 'rb') as f:
                    if media_type == "audio":
                        await context.bot.send_audio(chat_id, f, title="Audio", caption="✅ Done")
                    else:
                        await context.bot.send_video(chat_id, f, caption="✅ Done")
                
                if graph:
                    await context.bot.send_photo(chat_id, graph)
                
                os.remove(fpath)
                await context.bot.delete_message(chat_id, msg_id)
            except Exception as e:
                await context.bot.send_message(chat_id, f"❌ Upload error: {e}")
        else:
            await context.bot.send_message(chat_id, f"❌ Download failed: {snap.get('error')}")
            
    if task_id in download_tasks: del download_tasks[task_id]

# ============================================================================
# APP LIFESPAN
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    if BOT_TOKEN:
        global ptb_application
        ptb_application = Application.builder().token(BOT_TOKEN).build()
        ptb_application.add_handler(CommandHandler("start", start))
        ptb_application.add_handler(MessageHandler(filters.TEXT, handle_link))
        ptb_application.add_handler(CallbackQueryHandler(button_callback))
        
        await ptb_application.initialize()
        await ptb_application.start()
        await ptb_application.updater.start_polling()
        logger.info("✅ Bot Started")
        yield
        await ptb_application.updater.stop()
        await ptb_application.stop()
        await ptb_application.shutdown()
    else:
        yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
def home():
    return {"status": "active"}
