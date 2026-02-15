import os
import sys
import asyncio
import logging
import threading
import uuid
import time
import shutil
import io
from contextlib import asynccontextmanager

# 1. Nest Asyncio (Must be first)
import nest_asyncio
nest_asyncio.apply()

# FastAPI
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import BadRequest

# Downloading
import yt_dlp
import aiohttp

# Matplotlib setup
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# ============================================================================
# CONFIGURATION
# ============================================================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DOWNLOAD_DIR = "downloads"
BIN_PATH = os.path.join(os.getcwd(), "bin")
os.environ["PATH"] += os.pathsep + BIN_PATH

# Clean up start
if os.path.exists(DOWNLOAD_DIR):
    shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Global State
download_tasks = {}
active_downloads = {}
ptb_application = None

# ============================================================================
# UTILS & YT-DLP HELPERS
# ============================================================================

def get_ydl_opts(basic=True):
    """Returns configured options to bypass YouTube blocks"""
    opts = {
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'ignoreerrors': True,
        'logtostderr': False,
        'source_address': '0.0.0.0', # Force IPv4
        # Browser Impersonation to fix "No Formats" issue
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'ffmpeg_location': BIN_PATH,
    }
    return opts

async def get_formats_safe(url: str, media_type: str):
    """Robust format fetcher"""
    loop = asyncio.get_running_loop()

    def _fetch():
        opts = get_ydl_opts()
        opts.update({
            'extract_flat': False, # We need full info
            'noplaylist': True,
        })
        
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info: return []
                
                formats = info.get("formats", [])
                
                # Filter logic
                valid_formats = []
                for f in formats:
                    # Skip m3u8/dash manifest formats usually
                    if 'manifest' in f.get('url', ''): continue
                    
                    if media_type == "audio":
                        # Look for audio-only
                        if f.get('vcodec') == 'none' and f.get('acodec') != 'none':
                            valid_formats.append(f)
                    else:
                        # Look for video (mp4 preferred)
                        if f.get('vcodec') != 'none' and f.get('ext') == 'mp4':
                            valid_formats.append(f)
                            
                return valid_formats
        except Exception as e:
            logger.error(f"Format fetch error: {e}")
            return []

    return await loop.run_in_executor(None, _fetch)

class ProgressTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.percent = 0.0
        self.speed = 0.0
        self.status = "running"
        self.filename = None
        self.error = None
        self.speed_history = []
        self.start_time = time.time()

    def update(self, d):
        with self.lock:
            if d['status'] == 'downloading':
                total = d.get('total_bytes') or d.get('total_bytes_estimate') or 1
                downloaded = d.get('downloaded_bytes', 0)
                self.percent = (downloaded / total) * 100
                self.speed = d.get('speed', 0) or 0
                if self.speed > 0:
                    self.speed_history.append((time.time() - self.start_time, self.speed))
            elif d['status'] == 'finished':
                self.status = "finished"
                self.filename = d.get('filename')
                self.percent = 100
            elif d['status'] == 'error':
                self.status = "error"
                self.error = "Download Error"

    def get_snap(self):
        with self.lock:
            return {
                "percent": self.percent,
                "speed": self.speed,
                "status": self.status,
                "filename": self.filename,
                "error": self.error,
                "history": list(self.speed_history)
            }

def download_worker(task_id, url, format_id, media_type):
    tracker = ProgressTracker()
    download_tasks[task_id]["tracker"] = tracker
    
    # Output path
    out_tmpl = os.path.join(DOWNLOAD_DIR, f"{task_id}_%(title)s.%(ext)s")

    opts = get_ydl_opts()
    opts.update({
        'outtmpl': out_tmpl,
        'format': format_id if format_id != "best" else ("bestaudio/best" if media_type == "audio" else "bestvideo+bestaudio/best"),
        'progress_hooks': [tracker.update],
    })

    if media_type == "audio":
        opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]
    else:
        # Merge video+audio if needed
        opts['postprocessors'] = [{'key': 'FFmpegVideoConvertor', 'preferedformat': 'mp4'}]

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
    except Exception as e:
        logger.error(f"DL Worker Error: {e}")
        tracker.update({'status': 'error', 'error': str(e)})
    
    download_tasks[task_id]["done"] = True

# ============================================================================
# BOT HANDLERS
# ============================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 **Hi!** Send me a YouTube link to download.")

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message.text
    if "youtu" in msg:
        active_downloads[update.effective_chat.id] = {"url": msg}
        kb = [
            [InlineKeyboardButton("🎵 Audio (MP3)", callback_data="audio"),
             InlineKeyboardButton("🎬 Video (MP4)", callback_data="video")]
        ]
        await update.message.reply_text("Select Format:", reply_markup=InlineKeyboardMarkup(kb))
    else:
        await update.message.reply_text("❌ Invalid Link.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() # Vital to stop loading animation
    
    chat_id = query.message.chat.id
    data = query.data

    if chat_id not in active_downloads:
        await query.edit_message_text("⚠️ Link expired. Please send it again.")
        return

    url = active_downloads[chat_id]["url"]

    if data in ["audio", "video"]:
        await query.edit_message_text(f"🔍 **Fetching {data} formats...**\n(Please wait, this might take a few seconds)")
        
        # Fetch formats
        formats = await get_formats_safe(url, data)
        
        kb = []
        if formats:
            # Sort and deduplicate
            seen = set()
            count = 0
            # Sort: Audio by bitrate, Video by height
            formats.sort(key=lambda x: x.get('tbr', 0) if data == "audio" else x.get('height', 0), reverse=True)
            
            for f in formats:
                fid = f['format_id']
                if fid in seen: continue
                
                if data == "audio":
                    abr = f.get('abr') or f.get('tbr') or 0
                    label = f"MP3 - {int(abr)} kbps"
                else:
                    h = f.get('height')
                    if not h: continue
                    label = f"MP4 - {h}p"
                
                kb.append([InlineKeyboardButton(label, callback_data=f"dl_{data}_{fid}")])
                seen.add(fid)
                count += 1
                if count >= 6: break # Max 6 options
        
        # Always add a "Best Quality" fallback button
        kb.insert(0, [InlineKeyboardButton(f"🚀 Best Quality (Auto)", callback_data=f"dl_{data}_best")])
        kb.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])

        await query.edit_message_text(
            f"Select {data} quality:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif data.startswith("dl_"):
        _, type_, fid = data.split("_", 2)
        task_id = str(uuid.uuid4())
        
        download_tasks[task_id] = {"done": False}
        
        # Start Thread
        t = threading.Thread(target=download_worker, args=(task_id, url, fid, type_))
        t.daemon = True
        t.start()
        
        await query.edit_message_text("⏳ **Initializing download...**")
        asyncio.create_task(monitor_progress(chat_id, query.message.id, task_id, context, type_))

    elif data == "cancel":
        await query.edit_message_text("🚫 Cancelled")
        if chat_id in active_downloads: del active_downloads[chat_id]

async def monitor_progress(chat_id, msg_id, task_id, context, mtype):
    last_text = ""
    start_time = time.time()
    
    while True:
        await asyncio.sleep(2)
        task = download_tasks.get(task_id)
        if not task: break
        
        tracker = task.get("tracker")
        if not tracker: continue
        
        snap = tracker.get_snap()
        status = snap['status']
        
        if status in ['finished', 'error']:
            break
            
        # UI Update (Limit to every 3s)
        if time.time() - start_time > 3:
            p = snap['percent']
            s = snap['speed'] / 1000000 # MB/s
            text = f"⬇️ **Downloading...**\nExample: `720p`\nProgress: `{p:.1f}%`\nSpeed: `{s:.2f} MB/s`"
            if text != last_text:
                try:
                    await context.bot.edit_message_text(chat_id, msg_id, text=text, parse_mode='Markdown')
                    last_text = text
                    start_time = time.time()
                except BadRequest: pass

    # Upload Phase
    task = download_tasks.get(task_id)
    if task and task.get('done'):
        tracker = task['tracker']
        snap = tracker.get_snap()
        
        if snap['status'] == 'finished' and snap['filename']:
            fpath = snap['filename']
            # Audio fix
            if mtype == 'audio' and not fpath.endswith('.mp3'):
                base = os.path.splitext(fpath)[0]
                if os.path.exists(base + ".mp3"): fpath = base + ".mp3"
            
            if os.path.exists(fpath):
                await context.bot.edit_message_text(chat_id, msg_id, text="🚀 **Uploading to Telegram...**")
                try:
                    with open(fpath, 'rb') as f:
                        if mtype == 'audio':
                            await context.bot.send_audio(chat_id, f, caption="✅ Downloaded via Bot")
                        else:
                            await context.bot.send_video(chat_id, f, caption="✅ Downloaded via Bot")
                    await context.bot.delete_message(chat_id, msg_id)
                except Exception as e:
                    await context.bot.send_message(chat_id, f"❌ Upload Failed: {e}")
                finally:
                    os.remove(fpath) # Cleanup
            else:
                await context.bot.send_message(chat_id, "❌ File lost after download.")
        else:
            await context.bot.send_message(chat_id, "❌ Download Failed.")
            
    if task_id in download_tasks: del download_tasks[task_id]

# ============================================================================
# APP LIFESPAN
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    if not BOT_TOKEN:
        logger.error("No Token")
        yield
        return
        
    global ptb_application
    ptb_application = Application.builder().token(BOT_TOKEN).build()
    ptb_application.add_handler(CommandHandler("start", start))
    ptb_application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    ptb_application.add_handler(CallbackQueryHandler(button_callback))
    
    await ptb_application.initialize()
    await ptb_application.start()
    await ptb_application.updater.start_polling()
    
    yield
    
    await ptb_application.updater.stop()
    await ptb_application.stop()
    await ptb_application.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def home():
    return {"status": "Bot is running with Robust YT-DLP"}
