import os, time, threading, queue, shutil, sys
import requests
from flask import Flask, request
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.console import Group
from rich.columns import Columns
from rich.text import Text
from rich import box
from collections import deque
import logging

# ─────────────────────────────────────────────
# SILENCE FLASK LOGS
# ─────────────────────────────────────────────
logging.getLogger("werkzeug").setLevel(logging.ERROR)

import json

if not os.path.exists("config.json"):
    print("❌ Config not found. Run: python config.py first")
    exit()

with open("config.json") as f:
    cfg = json.load(f)

SYNC_ROOT = cfg["SYNC_ROOT"]
PEER = cfg["PEER"]
PORT = cfg["PORT"]
CHUNK_SIZE = cfg["CHUNK_SIZE"]
TIMEOUT = cfg["TIMEOUT"]
PEER_TIMEOUT = cfg["PEER_TIMEOUT"]

app = Flask(__name__)

upload_queue = queue.Queue()
progress_map = {}
activity = deque(maxlen=12)
IGNORE = set()

paused = False
running = True
last_seen = 0

state_lock = threading.Lock()

COLOR = {
    "UPLOAD": "green",
    "DELETE": "red",
    "RENAME": "yellow",
    "HELLO": "cyan",
    "PAUSE": "magenta",
    "RESUME": "green",
    "ERROR": "bold red",
    "EXIT": "bright_red"
}

# ================= HELPERS =================
def rel(p): return os.path.relpath(p, SYNC_ROOT)
def full(p): return os.path.join(SYNC_ROOT, p)

def safe_post(url, **kw):
    while running:
        try:
            return requests.post(url, timeout=TIMEOUT, **kw)
        except:
            time.sleep(1)

def log_event(t, msg):
    with state_lock:
        activity.appendleft((t, msg))
    try:
        requests.post(f"{PEER}/activity", json={"type": t, "msg": msg}, timeout=1)
    except:
        pass

def fmt_bytes(n):
    for u in ['B','KB','MB','GB']:
        if n < 1024: return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}TB"

def fmt_time(sec):
    if sec <= 0: return "∞"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"

# ================= SERVER ==================
@app.route("/hello", methods=["POST"])
def hello():
    global last_seen
    last_seen = time.time()
    log_event("HELLO", request.remote_addr)
    return "OK"

@app.route("/chunk", methods=["POST"])
def chunk():
    p = request.args["path"]
    IGNORE.add(p)
    path = full(p)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "ab") as f:
        f.write(request.data)
    return "OK"

@app.route("/delete", methods=["POST"])
def delete():
    p = request.json["path"]
    IGNORE.add(p)
    path = full(p)
    if os.path.exists(path):
        shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
    log_event("DELETE", p)
    return "OK"

@app.route("/rename", methods=["POST"])
def rename():
    o = request.json["old"]
    n = request.json["new"]
    IGNORE.update({o, n})
    os.makedirs(os.path.dirname(full(n)), exist_ok=True)
    os.rename(full(o), full(n))
    log_event("RENAME", f"{o} → {n}")
    return "OK"

@app.route("/activity", methods=["POST"])
def recv_activity():
    d = request.json
    with state_lock:
        activity.appendleft((d["type"], d["msg"]))
    return "OK"

# ================= SYNC CORE =================
def upload_file(path):
    local = full(path)
    if not os.path.exists(local):
        return

    size = os.path.getsize(local)

    with state_lock:
        progress_map[path] = {"sent": 0, "total": size, "start": time.time()}

    with open(local, "rb") as f:
        while running:
            if paused:
                time.sleep(0.2)
                continue

            data = f.read(CHUNK_SIZE)
            if not data:
                break

            safe_post(f"{PEER}/chunk", params={"path": path}, data=data)

            with state_lock:
                progress_map[path]["sent"] += len(data)

    log_event("UPLOAD", path)
    with state_lock:
        progress_map.pop(path, None)

def worker():
    while running:
        try:
            job = upload_queue.get(timeout=1)
        except queue.Empty:
            continue

        try:
            if job[0] == "UPLOAD":
                upload_file(job[1])
            elif job[0] == "DELETE":
                safe_post(f"{PEER}/delete", json={"path": job[1]})
            elif job[0] == "RENAME":
                safe_post(f"{PEER}/rename", json={"old": job[1], "new": job[2]})
        except Exception as e:
            log_event("ERROR", str(e))

        upload_queue.task_done()

# ================= WATCHDOG =================
class Handler(FileSystemEventHandler):
    def on_created(self, e):
        if not e.is_directory:
            p = rel(e.src_path)
            if p not in IGNORE:
                upload_queue.put(("UPLOAD", p))

    def on_modified(self, e):
        if not e.is_directory:
            p = rel(e.src_path)
            if p not in IGNORE:
                upload_queue.put(("UPLOAD", p))

    def on_deleted(self, e):
        p = rel(e.src_path)
        if p not in IGNORE:
            upload_queue.put(("DELETE", p))

    def on_moved(self, e):
        a, b = rel(e.src_path), rel(e.dest_path)
        if a not in IGNORE:
            upload_queue.put(("RENAME", a, b))

# ================= HEARTBEAT =================
def heartbeat():
    while running:
        try:
            requests.post(f"{PEER}/hello", timeout=2)
        except:
            pass
        time.sleep(3)

# ================= KEYBOARD =================
def key_listener():
    global paused, running
    import termios, tty

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)

    try:
        tty.setcbreak(fd)
        while running:
            k = sys.stdin.read(1).lower()
            if k == "p":
                paused = True
                log_event("PAUSE", "Paused")
            elif k == "r":
                paused = False
                log_event("RESUME", "Resumed")
            elif k == "q":
                log_event("EXIT", "Shutting down")
                running = False
                break
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)

# ================= TUI =================
def render_ui():
    with Live(screen=True, refresh_per_second=8) as live:
        while running:
            with state_lock:
                act = list(activity)[:3]
                prog = dict(progress_map)

            online = (time.time() - last_seen) < PEER_TIMEOUT

            # Header
            header = Panel(
                Text("VELOCITY", justify="center", style="bold cyan"),
                box=box.DOUBLE
            )

            # Left info
            speed = sum(
                d["sent"] / max(time.time() - d["start"], 0.1)
                for d in prog.values()
            )

            info = Panel(
                Text(
                    f"Root: {SYNC_ROOT}\n"
                    f"My IP: {PEER.split('//')[1]}\n"
                    f"Speed: {fmt_bytes(speed)}/s\n"
                    f"Queue: {upload_queue.qsize()}",
                    style="bold"
                ),
                title="INFO",
                box=box.ROUNDED
            )

            status = Panel(
                Text(
                    f"{'💚 ONLINE' if online else '❤️ OFFLINE'}\nFront IP: {PEER.split('//')[1]}",
                    style="bold green" if online else "bold red"
                ),
                title="STATUS",
                box=box.ROUNDED
            )

            act_table = Table(box=box.ROUNDED, expand=True)
            act_table.add_column("Type", width=8)
            act_table.add_column("Info")

            for t, m in act:
                act_table.add_row(f"[{COLOR.get(t,'white')}]{t}[/]", m)

            top = Columns([info, Group(status, act_table)], expand=True)

            uploads = []
            now = time.time()
            for p, d in prog.items():
                pct = int(d["sent"] / d["total"] * 100)
                speed = d["sent"] / max(now - d["start"], 0.1)
                eta = (d["total"] - d["sent"]) / speed if speed else 0

                bar = f"[{'█'*(pct//5)}{'░'*(20-pct//5)}] {pct}%"
                uploads.append(
                    Panel(
                        f"{p}\n{bar} {fmt_bytes(speed)}/s ETA {fmt_time(eta)}",
                        box=box.ROUNDED,
                        style="bright_green"
                    )
                )

            uploads_view = Columns(uploads, expand=True) if uploads else Text("No uploads", style="dim")

            footer = Panel(
                Text("Controls: [p] Pause   [r] Resume   [q] Quit",
                     justify="center", style="bold"),
                box=box.ROUNDED
            )

            live.update(Group(header, top, uploads_view, footer))
            time.sleep(0.15)

# ================= MAIN =================
if __name__ == "__main__":
    os.makedirs(SYNC_ROOT, exist_ok=True)

    threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=key_listener, daemon=True).start()

    threading.Thread(
        target=lambda: app.run("0.0.0.0", PORT, debug=False, use_reloader=False),
        daemon=True
    ).start()

    obs = Observer()
    obs.schedule(Handler(), SYNC_ROOT, recursive=True)
    obs.start()

    # UI LAST
    render_ui()
