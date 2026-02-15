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
from datetime import datetime
from typing import Dict, Optional, List, Any
from contextlib import asynccontextmanager

# FastAPI imports
from fastapi import FastAPI, HTTPException, BackgroundTasks
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
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import aiohttp
import requests

# ============================================================================
# CONFIGURATION – READ FROM ENVIRONMENT VARIABLES
# ============================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable not set")

SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("PORT", 8000))  # Render assigns PORT
DOWNLOAD_DIR = "downloads"

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE SETUP
# ============================================================================
conn = sqlite3.connect("users.db", check_same_thread=False)
c = conn.cursor()
c.execute("""CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    last_url TEXT,
    last_format TEXT,
    lang TEXT DEFAULT 'bn'
)""")
conn.commit()

# ============================================================================
# DOWNLOAD SERVER (FASTAPI)
# ============================================================================
download_tasks: Dict[str, Dict] = {}

class DownloadRequest(BaseModel):
    url: str
    format_id: str
    media_type: str  # 'audio' or 'video'

class ProgressTracker:
    """Thread-safe progress tracker for a single download"""
    def __init__(self):
        self.lock = threading.Lock()
        self.percent = 0.0
        self.speed = 0.0  # bytes/sec
        self.eta = 0
        self.downloaded = 0
        self.total = 0
        self.status = "downloading"  # downloading, finished, error
        self.filename = None
        self.error = None
        self.speed_history = []  # for graph
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
                self.percent = (self.downloaded / self.total) * 100 if self.total else 0
                self.speed = d.get("speed", 0)
                self.eta = d.get("eta", 0)
                # Record speed for graph (every second)
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
    """Run yt-dlp in a separate thread, update progress in download_tasks[task_id]"""
    tracker = ProgressTracker()
    download_tasks[task_id]["tracker"] = tracker

    def progress_hook(d):
        tracker.update(d)

    # Determine output template
    output_template = os.path.join(DOWNLOAD_DIR, "%(title)s.%(ext)s")
    
    ydl_opts = {
        "format": format_id,
        "outtmpl": output_template,
        "progress_hooks": [progress_hook],
        "quiet": True,
        "no_warnings": True,
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
        tracker.update({"status": "error", "error": str(e)})
    finally:
        download_tasks[task_id]["done"] = True

# Create FastAPI app
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Download server starting up...")
    yield
    # Shutdown
    logger.info("Download server shutting down...")
    # Clean up tasks
    download_tasks.clear()

app = FastAPI(title="YouTube Download Server", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "YouTube Download Server is running", "status": "active"}

@app.post("/download")
async def start_download(req: DownloadRequest):
    """Start a new download task and return task_id"""
    task_id = str(uuid.uuid4())
    download_tasks[task_id] = {
        "req": req.dict(),
        "done": False,
        "tracker": None
    }
    logger.info(f"Starting download task {task_id} for URL: {req.url}")
    
    # Run download in background thread
    thread = threading.Thread(target=download_worker, args=(task_id, req.url, req.format_id, req.media_type))
    thread.daemon = True
    thread.start()
    
    return JSONResponse({
        "task_id": task_id,
        "status": "started",
        "message": "Download started successfully"
    })

@app.get("/progress/{task_id}")
async def progress_stream(task_id: str):
    """SSE stream for real-time progress"""
    if task_id not in download_tasks:
        raise HTTPException(status_code=404, detail="Task not found")

    async def event_generator():
        last_sent = None
        consecutive_no_change = 0
        max_no_change = 5  # If no change for 5 seconds, still send keepalive
        
        while True:
            task = download_tasks.get(task_id)
            if not task:
                break
                
            tracker = task.get("tracker")
            if tracker:
                snap = tracker.snapshot()
                # Send only if changed significantly or every 5 seconds for keepalive
                should_send = False
                
                if snap != last_sent:
                    should_send = True
                    consecutive_no_change = 0
                else:
                    consecutive_no_change += 1
                    if consecutive_no_change >= 5:  # Send keepalive
                        should_send = True
                        consecutive_no_change = 0
                
                if should_send:
                    yield f"data: {json.dumps(snap)}\n\n"
                    last_sent = snap
                    
                if snap["status"] in ("finished", "error"):
                    # Also send final state
                    yield f"data: {json.dumps(snap)}\n\n"
                    break
            else:
                # No tracker yet, send placeholder
                yield f"data: {json.dumps({'status': 'initializing', 'percent': 0})}\n\n"
                
            await asyncio.sleep(1)  # update every second

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/result/{task_id}")
async def get_result(task_id: str):
    """Get final result (file path or error) after download finishes"""
    task = download_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.get("done"):
        raise HTTPException(status_code=400, detail="Download not finished yet")
    
    tracker = task.get("tracker")
    if tracker:
        snap = tracker.snapshot()
        if snap["status"] == "finished" and snap["filename"]:
            # Check if file exists (audio conversion might have changed extension)
            filename = snap["filename"]
            if os.path.exists(filename):
                return JSONResponse({
                    "filename": filename,
                    "status": "success",
                    "filesize": os.path.getsize(filename)
                })
            else:
                # Try with mp3 extension for audio
                base, ext = os.path.splitext(filename)
                mp3_path = base + ".mp3"
                if os.path.exists(mp3_path):
                    return JSONResponse({
                        "filename": mp3_path,
                        "status": "success",
                        "filesize": os.path.getsize(mp3_path)
                    })
                else:
                    return JSONResponse({
                        "status": "error",
                        "error": "File not found after download"
                    }, status_code=500)
        elif snap["status"] == "error":
            return JSONResponse({
                "status": "error",
                "error": snap["error"]
            }, status_code=500)
    
    return JSONResponse({
        "status": "error",
        "error": "Unknown state"
    }, status_code=500)

@app.post("/cancel/{task_id}")
async def cancel_download(task_id: str):
    """Cancel a running download"""
    if task_id in download_tasks:
        # Mark as cancelled and remove
        logger.info(f"Cancelling download task {task_id}")
        del download_tasks[task_id]
        return JSONResponse({"status": "cancelled", "message": "Download cancelled"})
    raise HTTPException(status_code=404, detail="Task not found")

@app.get("/tasks")
async def list_tasks():
    """List all active tasks"""
    active_tasks = {}
    for task_id, task in download_tasks.items():
        if not task.get("done"):
            tracker = task.get("tracker")
            if tracker:
                snap = tracker.snapshot()
                active_tasks[task_id] = {
                    "status": snap["status"],
                    "percent": snap["percent"],
                    "url": task["req"]["url"][:50] + "..." if len(task["req"]["url"]) > 50 else task["req"]["url"]
                }
            else:
                active_tasks[task_id] = {"status": "initializing"}
    return JSONResponse(active_tasks)

# ============================================================================
# TELEGRAM BOT
# ============================================================================

# Active downloads storage for bot
active_downloads: Dict[int, dict] = {}

async def delete_message_safe(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Delete a message safely ignoring errors"""
    try:
        await context.bot.delete_message(chat_id, message_id)
    except Exception as e:
        logger.debug(f"Failed to delete message: {e}")

async def get_formats(url: str, filter_type: str) -> List[Dict]:
    """Fetch formats using yt-dlp"""
    loop = asyncio.get_event_loop()
    
    def _fetch():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                all_formats = info.get("formats", [])
                
                if filter_type == "audio":
                    # Audio only formats
                    return [f for f in all_formats if f.get("vcodec") == "none" and f.get("acodec") != "none"]
                else:
                    # Video formats (with video codec)
                    # Include both combined and video-only formats
                    video_formats = [f for f in all_formats if f.get("vcodec") != "none"]
                    return video_formats
        except Exception as e:
            logger.error(f"Error fetching formats: {e}")
            return []
    
    return await loop.run_in_executor(None, _fetch)

def generate_speed_graph(speed_history: List[tuple]) -> Optional[io.BytesIO]:
    """Generate a matplotlib graph of speed over time"""
    if len(speed_history) < 2:
        return None
    
    try:
        times, speeds = zip(*speed_history)
        speeds_mbps = [s / 1_000_000 for s in speeds]  # Convert to Mbps
        
        plt.figure(figsize=(12, 6))
        plt.plot(times, speeds_mbps, marker='o', linestyle='-', color='#00ff00', linewidth=2, markersize=4)
        plt.fill_between(times, speeds_mbps, alpha=0.3, color='#00ff00')
        
        plt.xlabel("Time (seconds)", fontsize=12, fontweight='bold')
        plt.ylabel("Speed (Mbps)", fontsize=12, fontweight='bold')
        plt.title("Download Speed Timeline", fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3)
        plt.gca().set_facecolor('#1a1a1a')
        plt.gcf().patch.set_facecolor('#2a2a2a')
        plt.tick_params(colors='white')
        plt.gca().xaxis.label.set_color('white')
        plt.gca().yaxis.label.set_color('white')
        plt.gca().title.set_color('white')
        
        # Add average line
        avg_speed = sum(speeds_mbps) / len(speeds_mbps)
        plt.axhline(y=avg_speed, color='red', linestyle='--', alpha=0.7, label=f'Average: {avg_speed:.2f} Mbps')
        plt.legend(facecolor='#2a2a2a', edgecolor='white', labelcolor='white')
        
        buf = io.BytesIO()
        plt.savefig(buf, format='png', facecolor='#2a2a2a', edgecolor='none', bbox_inches='tight')
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Error generating graph: {e}")
        return None

async def stream_progress(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          chat_id: int, msg_id: int, task_id: str, media_type: str):
    """Connect to server's SSE stream and update message every second"""
    # Use localhost because both server and bot run in the same container
    server_url = f"http://localhost:{SERVER_PORT}/progress/{task_id}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(server_url) as resp:
                if resp.status != 200:
                    await context.bot.send_message(chat_id, "❌ Could not connect to progress stream.")
                    return
                
                # Buffer for speed data for graph
                speed_history = []
                last_update_time = 0
                
                async for line_bytes in resp.content:
                    line = line_bytes.decode().strip()
                    if line.startswith("data:"):
                        data_str = line[5:].strip()
                        if data_str:
                            try:
                                snap = json.loads(data_str)
                            except json.JSONDecodeError:
                                continue
                            
                            # Save speed data for graph
                            if snap.get("speed") and snap.get("speed_history"):
                                speed_history = snap["speed_history"]
                            
                            # Throttle updates to avoid flood limits (max once per second)
                            current_time = time.time()
                            if current_time - last_update_time < 1.0 and snap.get("status") == "downloading":
                                continue
                            
                            last_update_time = current_time
                            
                            # Build progress text
                            percent = snap.get("percent", 0)
                            downloaded_bytes = snap.get("downloaded", 0)
                            total_bytes = snap.get("total", 0)
                            
                            downloaded_mb = downloaded_bytes / (1024 * 1024)
                            total_mb = total_bytes / (1024 * 1024) if total_bytes else 0
                            
                            speed_bps = snap.get("speed", 0)
                            speed_mbps = speed_bps / 1_000_000  # Mbps
                            speed_mbs = speed_bps / (1024 * 1024)  # MB/s
                            
                            eta = snap.get("eta", 0)
                            status = snap.get("status", "unknown")
                            
                            # Progress bar
                            bar_length = 20
                            filled_length = int(bar_length * percent / 100)
                            bar = '█' * filled_length + '░' * (bar_length - filled_length)
                            
                            text = f"📥 **Downloading...**\n\n"
                            text += f"{bar} `{percent:.1f}%`\n\n"
                            text += f"📦 **Size:** `{downloaded_mb:.2f} MB / {total_mb:.2f} MB`\n"
                            
                            if speed_bps:
                                text += f"🚀 **Speed:** `{speed_mbps:.2f} Mbps` ({speed_mbs:.2f} MB/s)\n"
                            
                            if eta:
                                minutes = int(eta // 60)
                                seconds = int(eta % 60)
                                if minutes > 0:
                                    text += f"⏳ **ETA:** `{minutes}m {seconds}s`\n"
                                else:
                                    text += f"⏳ **ETA:** `{seconds}s`\n"
                            
                            # Add estimated time remaining
                            if speed_bps > 0 and total_bytes > downloaded_bytes:
                                remaining_bytes = total_bytes - downloaded_bytes
                                est_time = remaining_bytes / speed_bps
                                est_minutes = int(est_time // 60)
                                est_seconds = int(est_time % 60)
                                if est_minutes > 0:
                                    text += f"⌛ **Est. remaining:** `{est_minutes}m {est_seconds}s`\n"
                                else:
                                    text += f"⌛ **Est. remaining:** `{est_seconds}s`\n"
                            
                            # Add download speed classification
                            if speed_mbps > 10:
                                text += f"⚡ **Connection:** `Very Fast`\n"
                            elif speed_mbps > 5:
                                text += f"⚡ **Connection:** `Fast`\n"
                            elif speed_mbps > 2:
                                text += f"⚡ **Connection:** `Good`\n"
                            elif speed_mbps > 1:
                                text += f"⚡ **Connection:** `Average`\n"
                            else:
                                text += f"⚡ **Connection:** `Slow`\n"
                            
                            try:
                                await context.bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=msg_id,
                                    text=text,
                                    parse_mode='Markdown'
                                )
                            except Exception as e:
                                logger.debug(f"Failed to edit message: {e}")
                            
                            # If finished or error, break
                            if status in ("finished", "error"):
                                break
            
            # After stream ends, get final file
            await asyncio.sleep(1)  # Give server time to finish
            
            async with session.get(f"http://localhost:{SERVER_PORT}/result/{task_id}") as resp:
                if resp.status == 200:
                    result = await resp.json()
                    filename = result.get("filename")
                    
                    if filename and os.path.exists(filename):
                        # Handle audio conversion extension change
                        if media_type == "audio" and not filename.endswith(".mp3"):
                            base, _ = os.path.splitext(filename)
                            mp3_path = base + ".mp3"
                            if os.path.exists(mp3_path):
                                filename = mp3_path
                        
                        # Send file
                        caption = f"✅ **Download Complete!**\n\n📁 `{os.path.basename(filename)}`\n📊 `{result.get('filesize', 0) / (1024*1024):.2f} MB`"
                        
                        try:
                            with open(filename, "rb") as f:
                                if media_type == "audio":
                                    await context.bot.send_audio(
                                        chat_id=chat_id,
                                        audio=f,
                                        caption=caption,
                                        title=os.path.splitext(os.path.basename(filename))[0],
                                        parse_mode='Markdown'
                                    )
                                else:
                                    await context.bot.send_video(
                                        chat_id=chat_id,
                                        video=f,
                                        caption=caption,
                                        supports_streaming=True,
                                        parse_mode='Markdown'
                                    )
                            
                            # Delete progress message
                            await delete_message_safe(context, chat_id, msg_id)
                            
                            # Generate and send speed graph if we have data
                            if speed_history:
                                graph_img = generate_speed_graph(speed_history)
                                if graph_img:
                                    await context.bot.send_photo(
                                        chat_id=chat_id,
                                        photo=graph_img,
                                        caption="📊 **Download Speed Graph**\n\nShows speed variation during download",
                                        parse_mode='Markdown'
                                    )
                            
                            # Send summary
                            if speed_history and len(speed_history) > 1:
                                speeds = [s for _, s in speed_history]
                                avg_speed = sum(speeds) / len(speeds) / 1_000_000  # Mbps
                                max_speed = max(speeds) / 1_000_000  # Mbps
                                min_speed = min(speeds) / 1_000_000  # Mbps
                                
                                summary = f"📈 **Download Summary**\n\n"
                                summary += f"⚡ **Average Speed:** `{avg_speed:.2f} Mbps`\n"
                                summary += f"🚀 **Peak Speed:** `{max_speed:.2f} Mbps`\n"
                                summary += f"🐢 **Min Speed:** `{min_speed:.2f} Mbps`\n"
                                summary += f"⏱️ **Total Time:** `{speed_history[-1][0]:.1f} seconds`"
                                
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=summary,
                                    parse_mode='Markdown'
                                )
                                
                        except Exception as e:
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"❌ Failed to send file: {str(e)}"
                            )
                        finally:
                            # Clean up file
                            try:
                                os.remove(filename)
                                # Also try to remove any converted files
                                if media_type == "audio" and filename.endswith(".mp3"):
                                    original = filename[:-4] + ".*"
                                    # Could clean up original but it's already handled by yt-dlp
                            except Exception as e:
                                logger.error(f"Failed to delete file: {e}")
                    else:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="❌ Download completed but file not found."
                        )
                else:
                    try:
                        error_data = await resp.json()
                        error_msg = error_data.get("error", "Unknown error")
                    except:
                        error_msg = "Failed to get download result"
                    
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"❌ Download failed: {error_msg}"
                    )
    
    except aiohttp.ClientError as e:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Connection error to download server: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error in stream_progress: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Unexpected error: {str(e)}"
        )
    finally:
        # Clean up active download
        if chat_id in active_downloads:
            del active_downloads[chat_id]

async def start_download_via_server(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                     url: str, format_id: str, media_type: str):
    """Send download request to server and start progress streaming"""
    query = update.callback_query
    chat_id = query.message.chat.id
    msg_id = query.message.message_id

    await query.edit_message_text("⏳ Initializing download...")

    # Request download from server
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://localhost:{SERVER_PORT}/download", json={
                "url": url,
                "format_id": format_id,
                "media_type": media_type
            }) as resp:
                if resp.status != 200:
                    await query.edit_message_text("❌ Server failed to start download.")
                    return
                data = await resp.json()
                task_id = data["task_id"]
                logger.info(f"Started download task {task_id} for user {chat_id}")
    except Exception as e:
        await query.edit_message_text(f"❌ Failed to connect to download server: {str(e)}")
        return

    active_downloads[chat_id]["task_id"] = task_id

    # Start listening to progress stream
    await stream_progress(update, context, chat_id, msg_id, task_id, media_type)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send welcome message"""
    user = update.effective_user
    welcome_text = (
        f"👋 **Welcome {user.first_name}!**\n\n"
        "I'm a **Premium YouTube Downloader Bot** that can download videos and audio from YouTube.\n\n"
        "**Features:**\n"
        "🎯 **Real-time progress** - See download progress every second\n"
        "📊 **Speed graphs** - Get detailed speed analytics after download\n"
        "🎵 **Audio extraction** - Download as MP3 with various bitrates\n"
        "🎬 **Video downloads** - Choose from multiple resolutions\n"
        "⚡ **High speed** - Optimized download server\n"
        "🛡️ **Safe & secure** - Files auto-delete after sending\n\n"
        "**How to use:**\n"
        "1️⃣ Send me any YouTube link\n"
        "2️⃣ Choose audio or video\n"
        "3️⃣ Select quality/resolution\n"
        "4️⃣ Watch real-time progress\n"
        "5️⃣ Get your file with speed graph!\n\n"
        "**Commands:**\n"
        "/cancel - Cancel current download\n"
        "/stats - View server statistics\n"
        "/help - Show this help message"
    )
    
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send help message"""
    help_text = (
        "📚 **Help & Commands**\n\n"
        "**How to download:**\n"
        "Simply send a YouTube link and follow the instructions.\n\n"
        "**Commands:**\n"
        "/start - Start the bot\n"
        "/cancel - Cancel current download\n"
        "/stats - View server statistics\n"
        "/help - Show this help\n\n"
        "**Features:**\n"
        "• **Audio formats** - MP3 with various bitrates\n"
        "• **Video formats** - Multiple resolutions up to 4K\n"
        "• **Real-time progress** - Live speed and ETA\n"
        "• **Speed graphs** - Visual download analytics\n"
        "• **Auto cleanup** - Files deleted after sending\n\n"
        "**Need help?** Contact @YourUsername"
    )
    
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show server statistics"""
    chat_id = update.effective_chat.id
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://localhost:{SERVER_PORT}/tasks") as resp:
                if resp.status == 200:
                    tasks = await resp.json()
                    active_count = len(tasks)
                    
                    stats_text = (
                        "📊 **Server Statistics**\n\n"
                        f"🟢 **Server Status:** `Online`\n"
                        f"📥 **Active Downloads:** `{active_count}`\n"
                        f"📁 **Download Directory:** `{DOWNLOAD_DIR}`\n"
                        f"⏱️ **Server Uptime:** `Calculating...`\n"
                        f"🔄 **API Status:** `Healthy`\n"
                    )
                    
                    if active_count > 0:
                        stats_text += f"\n**Current Tasks:**\n"
                        for task_id, task_info in list(tasks.items())[:5]:  # Show first 5
                            stats_text += f"• {task_info.get('url', 'Unknown')[:30]}... - {task_info.get('percent', 0):.1f}%\n"
                    
                    await update.message.reply_text(stats_text, parse_mode='Markdown')
                else:
                    await update.message.reply_text("❌ Could not fetch server stats.")
    except Exception as e:
        await update.message.reply_text(f"❌ Server connection failed: {str(e)}")

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process a YouTube link"""
    chat_id = update.effective_chat.id
    text = update.message.text

    # Simple YouTube link check
    if "youtube.com" in text or "youtu.be" in text or "youtu.be/" in text:
        # Delete user's message to keep chat clean
        await delete_message_safe(context, chat_id, update.message.message_id)

        # Cancel any previous active download
        if chat_id in active_downloads:
            # Optionally notify server to cancel
            task_id = active_downloads[chat_id].get("task_id")
            if task_id:
                try:
                    async with aiohttp.ClientSession() as session:
                        await session.post(f"http://localhost:{SERVER_PORT}/cancel/{task_id}")
                except:
                    pass
            del active_downloads[chat_id]

        # Store URL in DB
        try:
            c.execute("INSERT OR REPLACE INTO users (user_id, last_url) VALUES (?, ?)", (chat_id, text))
            conn.commit()
        except Exception as e:
            logger.error(f"Database error: {e}")

        # Ask for media type with nice formatting
        keyboard = [
            [InlineKeyboardButton("🎵 Audio (MP3)", callback_data="audio")],
            [InlineKeyboardButton("🎬 Video (MP4)", callback_data="video")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent = await context.bot.send_message(
            chat_id, 
            "📥 **Link received!**\n\nWhat would you like to download?",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        # Save message id for later updates
        active_downloads[chat_id] = {
            "progress_msg_id": sent.message_id,
            "url": text
        }
    else:
        await update.message.reply_text(
            "❌ Please send a valid YouTube link.\n"
            "Examples:\n"
            "• https://youtube.com/watch?v=...\n"
            "• https://youtu.be/..."
        )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all inline keyboard callbacks"""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    data = query.data

    if chat_id not in active_downloads or "url" not in active_downloads[chat_id]:
        await query.edit_message_text("⚠️ No URL found. Please send a link again.")
        return

    url = active_downloads[chat_id]["url"]

    if data == "audio":
        # Fetch audio formats
        await query.edit_message_text("🔍 Fetching audio formats...")
        formats = await get_formats(url, "audio")
        
        if not formats:
            await query.edit_message_text("❌ No audio formats available for this video.")
            return
        
        # Sort by bitrate descending and remove duplicates
        formats.sort(key=lambda f: f.get("abr", 0) or 0, reverse=True)
        
        # Remove duplicates by format_id
        seen = set()
        unique_formats = []
        for f in formats:
            if f["format_id"] not in seen:
                seen.add(f["format_id"])
                unique_formats.append(f)
        
        keyboard = []
        for f in unique_formats[:15]:  # Limit to 15 options
            abr = f.get("abr", "?")
            if abr == "?" or abr is None:
                abr = "Unknown"
            
            format_note = f.get("format_note", "")
            if format_note:
                label = f"{abr} kbps - {format_note}"
            else:
                label = f"{abr} kbps"
            
            keyboard.append([InlineKeyboardButton(label, callback_data=f'audio_{f["format_id"]}')])
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
        
        await query.edit_message_text(
            "🎵 **Select Audio Quality:**\n_(Higher bitrate = better quality)_",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    elif data == "video":
        await query.edit_message_text("🔍 Fetching video formats...")
        formats = await get_formats(url, "video")
        
        if not formats:
            await query.edit_message_text("❌ No video formats available for this video.")
            return
        
        # Sort by height descending and filter out formats without height
        valid_formats = [f for f in formats if f.get("height")]
        valid_formats.sort(key=lambda f: f.get("height", 0), reverse=True)
        
        # Remove duplicates by height
        seen_heights = set()
        unique_formats = []
        for f in valid_formats:
            height = f.get("height")
            if height and height not in seen_heights:
                seen_heights.add(height)
                unique_formats.append(f)
        
        keyboard = []
        for f in unique_formats[:15]:  # Limit to 15 options
            height = f.get("height", "?")
            fps = f.get("fps", "")
            format_note = f.get("format_note", "")
            
            if fps:
                label = f"{height}p {fps}fps"
            else:
                label = f"{height}p"
            
            if format_note:
                label += f" - {format_note}"
            
            keyboard.append([InlineKeyboardButton(label, callback_data=f'video_{f["format_id"]}')])
        
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
        
        await query.edit_message_text(
            "🎬 **Select Video Resolution:**\n_(Higher resolution = larger file)_",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

    elif data.startswith("audio_"):
        format_id = data.split("_", 1)[1]
        await start_download_via_server(update, context, url, format_id, "audio")

    elif data.startswith("video_"):
        format_id = data.split("_", 1)[1]
        await start_download_via_server(update, context, url, format_id, "video")

    elif data == "cancel":
        await query.edit_message_text("🚫 Operation cancelled.")
        await asyncio.sleep(2)
        await delete_message_safe(context, chat_id, query.message.message_id)
        if chat_id in active_downloads:
            del active_downloads[chat_id]

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel current download"""
    chat_id = update.effective_chat.id
    
    if chat_id in active_downloads:
        task_id = active_downloads[chat_id].get("task_id")
        
        if task_id:
            try:
                async with aiohttp.ClientSession() as session:
                    await session.post(f"http://localhost:{SERVER_PORT}/cancel/{task_id}")
            except Exception as e:
                logger.error(f"Error cancelling task: {e}")
        
        del active_downloads[chat_id]
        await update.message.reply_text("✅ Current download cancelled.")
    else:
        await update.message.reply_text("No active download to cancel.")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(f"Update {update} caused error {context.error}")
    
    try:
        if update and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="❌ An error occurred. Please try again later."
            )
    except:
        pass

# ============================================================================
# MAIN FUNCTION – RUN BOTH SERVER AND BOT CONCURRENTLY
# ============================================================================

async def run_bot():
    """Run the Telegram bot"""
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Add error handler
    application.add_error_handler(error_handler)

    # Start bot
    logger.info("Starting Telegram bot...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    logger.info("Bot is running!")
    
    # Keep running
    try:
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour
    except asyncio.CancelledError:
        logger.info("Stopping bot...")
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

def run_server():
    """Run FastAPI server – this will block, so run in a thread"""
    logger.info(f"Starting download server on {SERVER_HOST}:{SERVER_PORT}")
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT, log_level="info")

async def main():
    """Main function to run both server and bot concurrently"""
    logger.info("=" * 50)
    logger.info("Starting Premium YouTube Downloader")
    logger.info("=" * 50)
    
    # Run FastAPI server in a separate thread (uvicorn.run is blocking)
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Give server a moment to start
    await asyncio.sleep(2)
    
    # Run bot (this will run until cancelled)
    await run_bot()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
