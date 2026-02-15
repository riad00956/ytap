import os
import sys
import json
import asyncio
import logging
import threading
import sqlite3
import time
import uuid
import io
import shutil
from datetime import datetime
from typing import Dict, Optional, List, Any
from contextlib import asynccontextmanager

# FastAPI imports
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import aiohttp

# ============================================================================
# CONFIGURATION
# ============================================================================
# Render থেকে বা .env থেকে টোকেন নেবে
BOT_TOKEN = os.environ.get("BOT_TOKEN")
# লোকাল টেস্টের জন্য যদি এনভায়রনমেন্ট না থাকে (সতর্কতা: প্রোডাকশনে এটা সরাবেন)
if not BOT_TOKEN:
    print("WARNING: BOT_TOKEN not set. Bot will not start correctly.")

# Render পোর্টের জন্য সেটআপ
PORT = int(os.environ.get("PORT", 8000))
SERVER_URL = f"http://localhost:{PORT}" 

DOWNLOAD_DIR = "downloads"
# FFmpeg পাথ সেট করা (build.sh এর মাধ্যমে যেখানে ইন্সটল হয়েছে)
os.environ["PATH"] += os.pathsep + os.path.join(os.getcwd(), "bin")

# Ensure download directory exists
if os.path.exists(DOWNLOAD_DIR):
    shutil.rmtree(DOWNLOAD_DIR) # স্টার্টআপের সময় ক্লিন করা
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
# yt-dlp এর অতিরিক্ত লগ কমানো
logging.getLogger("yt_dlp").setLevel(logging.WARNING)

# ============================================================================
# GLOBAL STATE
# ============================================================================
download_tasks: Dict[str, Dict] = {}
active_downloads: Dict[int, dict] = {}
ptb_application: Optional[Application] = None  # টেলিগ্রাম অ্যাপ গ্লোবাল ভেরিয়েবল

# ============================================================================
# HELPER CLASSES & FUNCTIONS
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
        self.eta = 0
        self.downloaded = 0
        self.total = 0
        self.status = "downloading"
        self.filename = None
        self.error = None
        self.speed_history = []
        self.start_time = time.time()

    def update(self, d):
        with self.lock:
            if d["status"] == "downloading":
                self.status = "downloading"
                if d.get("total_bytes"):
                    self.total = d["total_bytes"]
                    self.downloaded = d.get("downloaded_bytes", 0)
                elif d.get("total_bytes_estimate"):
                    self.total = d["total_bytes_estimate"]
                    self.downloaded = d.get("downloaded_bytes", 0)
                
                if self.total:
                    self.percent = (self.downloaded / self.total) * 100
                
                self.speed = d.get("speed", 0)
                self.eta = d.get("eta", 0)
                
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
                "eta": self.eta,
                "downloaded": self.downloaded,
                "total": self.total,
                "status": self.status,
                "filename": self.filename,
                "error": self.error,
                "speed_history": self.speed_history.copy() if self.speed_history else []
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
        # FFmpeg location explicitly defined for safety
        "ffmpeg_location": os.path.join(os.getcwd(), "bin"),
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

# ============================================================================
# TELEGRAM BOT HANDLERS
# ============================================================================

async def get_formats(url: str, filter_type: str) -> List[Dict]:
    loop = asyncio.get_running_loop()
    
    def _fetch():
        ydl_opts = {
            "quiet": True, 
            "no_warnings": True,
            # Just extract info, don't download
            "extract_flat": False, 
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                formats = info.get("formats", [])
                
                if filter_type == "audio":
                    return [f for f in formats if f.get("vcodec") == "none" and f.get("acodec") != "none"]
                else:
                    return [f for f in formats if f.get("vcodec") != "none"]
        except Exception as e:
            logger.error(f"Error fetching formats: {e}")
            return []
    
    return await loop.run_in_executor(None, _fetch)

def generate_speed_graph(speed_history: List[tuple]) -> Optional[io.BytesIO]:
    if len(speed_history) < 2:
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
    except Exception:
        return None

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 **Hello!** I am a YouTube Downloader Bot on Render.\n"
        "Send me a YouTube link to start!",
        parse_mode='Markdown'
    )

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text

    if "youtube.com" in text or "youtu.be" in text:
        keyboard = [
            [InlineKeyboardButton("🎵 Audio (MP3)", callback_data="audio")],
            [InlineKeyboardButton("🎬 Video (MP4)", callback_data="video")],
        ]
        
        active_downloads[chat_id] = {"url": text}
        
        await update.message.reply_text(
            "📥 **Link received!** Select format:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("❌ Please send a valid YouTube link.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    data = query.data

    if chat_id not in active_downloads:
        await query.edit_message_text("⚠️ Session expired. Send link again.")
        return

    url = active_downloads[chat_id]["url"]

    if data in ["audio", "video"]:
        await query.edit_message_text(f"🔍 Fetching {data} formats...")
        formats = await get_formats(url, data)
        
        if not formats:
            await query.edit_message_text("❌ Failed to fetch formats.")
            return

        keyboard = []
        unique_ids = set()
        
        # Simplify format selection for better UX
        count = 0
        formats.sort(key=lambda x: x.get('filesize') or 0, reverse=True)
        
        for f in formats:
            fid = f['format_id']
            if fid in unique_ids: continue
            
            label = ""
            if data == "audio":
                abr = f.get('abr', 0)
                if abr: label = f"{int(abr)} kbps"
            else:
                h = f.get('height')
                if h: label = f"{h}p"
            
            if label:
                unique_ids.add(fid)
                keyboard.append([InlineKeyboardButton(label, callback_data=f"dl_{data}_{fid}")])
                count += 1
            
            if count >= 6: break # Limit options

        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
        await query.edit_message_text("Select Quality:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("dl_"):
        _, type_, fid = data.split("_", 2)
        await query.edit_message_text("🚀 Starting download...")
        
        # Start download via internal API call logic (Direct call for efficiency)
        task_id = str(uuid.uuid4())
        download_tasks[task_id] = {
            "req": {"url": url, "format_id": fid, "media_type": type_},
            "done": False,
            "tracker": None
        }
        
        # Start thread
        t = threading.Thread(target=download_worker, args=(task_id, url, fid, type_))
        t.daemon = True
        t.start()
        
        active_downloads[chat_id]["task_id"] = task_id
        await monitor_download(chat_id, query.message.id, task_id, context, type_)

    elif data == "cancel":
        if chat_id in active_downloads:
            del active_downloads[chat_id]
        await query.edit_message_text("🚫 Cancelled.")

async def monitor_download(chat_id, msg_id, task_id, context, media_type):
    last_text = ""
    start_time = time.time()
    
    while True:
        task = download_tasks.get(task_id)
        if not task: break
        
        tracker = task.get("tracker")
        if not tracker:
            await asyncio.sleep(1)
            continue
            
        snap = tracker.snapshot()
        status = snap["status"]
        
        if status in ["finished", "error"]:
            break
            
        # Update progress message every 2 seconds to avoid flood limits
        if time.time() - start_time > 2:
            pct = snap['percent']
            speed_mb = (snap['speed'] or 0) / 1000000
            text = f"📥 **Downloading...**\n`{pct:.1f}%` complete\n🚀 `{speed_mb:.2f} MB/s`"
            
            if text != last_text:
                try:
                    await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, parse_mode='Markdown')
                    last_text = text
                    start_time = time.time()
                except: pass
        
        await asyncio.sleep(1)

    # Handle completion
    if task and task.get("done"):
        snap = task["tracker"].snapshot()
        if snap["status"] == "finished" and snap["filename"]:
            fpath = snap["filename"]
            
            # Auto-correction for audio extension
            if media_type == "audio" and not os.path.exists(fpath):
                base, _ = os.path.splitext(fpath)
                if os.path.exists(base + ".mp3"):
                    fpath = base + ".mp3"
            
            if os.path.exists(fpath):
                await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="📤 Uploading to Telegram...")
                
                try:
                    # Graph
                    graph = generate_speed_graph(snap["speed_history"])
                    
                    with open(fpath, 'rb') as f:
                        if media_type == "audio":
                            await context.bot.send_audio(chat_id, f, title="Audio", caption="✅ Downloaded via Render Bot")
                        else:
                            await context.bot.send_video(chat_id, f, caption="✅ Downloaded via Render Bot")
                    
                    if graph:
                        await context.bot.send_photo(chat_id, graph, caption="📊 Speed Graph")
                    
                    # Cleanup
                    os.remove(fpath)
                    await context.bot.delete_message(chat_id, msg_id)
                    
                except Exception as e:
                    await context.bot.send_message(chat_id, f"❌ Upload failed: {e}")
            else:
                await context.bot.send_message(chat_id, "❌ File not found on server.")
        else:
            await context.bot.send_message(chat_id, f"❌ Error: {snap.get('error')}")
            
    # Clean memory
    if task_id in download_tasks:
        del download_tasks[task_id]

# ============================================================================
# FASTAPI APP & LIFESPAN (MAIN ENTRY POINT)
# ============================================================================

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    This handles the startup and shutdown of the Telegram Bot
    alongside the FastAPI server.
    """
    global ptb_application
    
    if not BOT_TOKEN:
        logger.error("No BOT_TOKEN found! Bot will not start.")
        yield
        return

    # Initialize Bot
    ptb_application = Application.builder().token(BOT_TOKEN).build()
    
    # Add Handlers
    ptb_application.add_handler(CommandHandler("start", start))
    ptb_application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    ptb_application.add_handler(CallbackQueryHandler(button_callback))
    
    # Start Bot
    logger.info("Starting Telegram Bot...")
    await ptb_application.initialize()
    await ptb_application.start()
    await ptb_application.updater.start_polling()
    
    yield
    
    # Shutdown Bot
    logger.info("Stopping Telegram Bot...")
    await ptb_application.updater.stop()
    await ptb_application.stop()
    await ptb_application.shutdown()

# Register lifespan
app = FastAPI(lifespan=lifespan)

# Add CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "running", "service": "Telegram Downloader Bot"}

@app.get("/health")
async def health():
    return {"status": "ok"}
