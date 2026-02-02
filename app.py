import json
import os
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, render_template, request, jsonify, Response
import yt_dlp

app = Flask(__name__)

# task_id -> task dict
tasks = {}

PARALLEL_WORKERS = 3


def _safe_download_path(raw_path):
    """Resolve and validate the download path to prevent traversal attacks."""
    expanded = os.path.expanduser(raw_path or "~/Downloads")
    resolved = os.path.realpath(expanded)
    home = os.path.realpath(os.path.expanduser("~"))
    if not resolved.startswith(home):
        return None
    return resolved


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/playlist-info", methods=["POST"])
def playlist_info():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "skip_download": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except yt_dlp.utils.DownloadError as e:
        return jsonify({"error": str(e)}), 400

    if info is None:
        return jsonify({"error": "Could not fetch playlist info"}), 400

    entries = info.get("entries") or []
    videos = []
    for entry in entries:
        if entry is None:
            continue
        thumbnail = ""
        if entry.get("thumbnails"):
            thumbnail = entry["thumbnails"][0].get("url", "")
        elif entry.get("thumbnail"):
            thumbnail = entry["thumbnail"]
        videos.append({
            "id": entry.get("id", ""),
            "title": entry.get("title", "Unknown"),
            "url": entry.get("url") or entry.get("webpage_url") or "",
            "thumbnail": thumbnail,
        })

    return jsonify({
        "title": info.get("title", "Unknown Playlist"),
        "video_count": len(videos),
        "videos": videos,
    })


@app.route("/api/download", methods=["POST"])
def start_download():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    fmt = data.get("format", "video")
    raw_path = data.get("path", "~/Downloads")
    video_indices = data.get("video_indices")  # list of 0-based ints, or None for all
    playlist_title = data.get("playlist_title", "Unknown Playlist")
    all_videos = data.get("videos", [])  # [{id, title}, ...]

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    # Block duplicate: reject if an active task for the same URL exists
    for existing in tasks.values():
        if existing["url"] == url and existing["status"] in ("starting", "downloading"):
            return jsonify({"error": "This playlist is already being downloaded"}), 409

    download_path = _safe_download_path(raw_path)
    if download_path is None:
        return jsonify({"error": "Invalid download path"}), 400

    task_id = uuid.uuid4().hex[:12]
    tasks[task_id] = {
        "status": "starting",
        "playlist_title": playlist_title,
        "videos": [],           # populated once download thread resolves entries
        "video_indices": [],     # which indices are being downloaded
        "progress": {},          # idx -> float (0-100, or -1 for failed)
        "cancelled": False,
        "paused": False,
        "error": None,
        # Store originals for retry
        "url": url,
        "format": fmt,
        "download_path": download_path,
    }

    thread = threading.Thread(
        target=_download_playlist,
        args=(task_id, url, fmt, download_path, video_indices, all_videos),
        daemon=True,
    )
    thread.start()

    return jsonify({"task_id": task_id})


@app.route("/api/progress/<task_id>")
def progress_stream(task_id):
    if task_id not in tasks:
        return jsonify({"error": "Unknown task"}), 404

    def generate():
        last_sent = None
        while True:
            task = tasks.get(task_id)
            if task is None:
                break

            snapshot = json.dumps({
                "status": task["status"],
                "progress": task["progress"],
                "videos": task["videos"],
                "video_indices": task["video_indices"],
                "error": task["error"],
            })

            if snapshot != last_sent:
                yield f"data: {snapshot}\n\n"
                last_sent = snapshot

            if task["status"] in ("done", "error", "cancelled", "paused"):
                break

            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/cancel/<task_id>", methods=["POST"])
def cancel_download(task_id):
    task = tasks.get(task_id)
    if task is None:
        return jsonify({"error": "Unknown task"}), 404
    task["cancelled"] = True
    return jsonify({"ok": True})


@app.route("/api/pause/<task_id>", methods=["POST"])
def pause_download(task_id):
    task = tasks.get(task_id)
    if task is None:
        return jsonify({"error": "Unknown task"}), 404
    if task["status"] not in ("starting", "downloading"):
        return jsonify({"error": "Task is not active"}), 400
    task["paused"] = True
    return jsonify({"ok": True})


@app.route("/api/resume/<task_id>", methods=["POST"])
def resume_download(task_id):
    task = tasks.get(task_id)
    if task is None:
        return jsonify({"error": "Unknown task"}), 404
    if task["status"] not in ("paused", "cancelled", "done"):
        return jsonify({"error": "Task cannot be resumed"}), 400

    # Collect indices where progress < 100 (incomplete: 0, -1, or partial)
    resume_indices = [
        idx for idx in task["video_indices"]
        if task["progress"].get(idx, 0) < 100
    ]

    if not resume_indices:
        return jsonify({"error": "All videos already completed"}), 400

    # Reset progress for incomplete videos
    for idx in resume_indices:
        task["progress"][idx] = 0.0
    task["paused"] = False
    task["cancelled"] = False
    task["error"] = None
    task["status"] = "downloading"

    thread = threading.Thread(
        target=_retry_videos,
        args=(task, resume_indices),
        daemon=True,
    )
    thread.start()

    return jsonify({"ok": True, "resuming": resume_indices})


@app.route("/api/tasks/<task_id>", methods=["DELETE"])
def delete_task(task_id):
    task = tasks.get(task_id)
    if task is None:
        return jsonify({"error": "Unknown task"}), 404
    if task["status"] not in ("done", "error", "cancelled", "paused"):
        return jsonify({"error": "Cannot delete a running task, cancel it first"}), 400
    del tasks[task_id]
    return jsonify({"ok": True})


@app.route("/api/retry/<task_id>", methods=["POST"])
def retry_failed(task_id):
    task = tasks.get(task_id)
    if task is None:
        return jsonify({"error": "Unknown task"}), 404
    if task["status"] not in ("done", "error", "cancelled", "paused"):
        return jsonify({"error": "Task is still running"}), 400

    data = request.get_json(silent=True) or {}
    retry_indices = data.get("video_indices")  # specific indices, or None for all failed

    if retry_indices is None:
        retry_indices = [
            idx for idx in task["video_indices"]
            if task["progress"].get(idx, 0) < 0
        ]

    if not retry_indices:
        return jsonify({"error": "No failed videos to retry"}), 400

    # Reset progress for retried videos and reopen the task
    for idx in retry_indices:
        task["progress"][idx] = 0.0
    task["cancelled"] = False
    task["error"] = None
    task["status"] = "downloading"

    thread = threading.Thread(
        target=_retry_videos,
        args=(task, retry_indices),
        daemon=True,
    )
    thread.start()

    return jsonify({"ok": True, "retrying": retry_indices})


@app.route("/api/browse")
def browse_directory():
    raw_path = request.args.get("path", "~")
    expanded = os.path.expanduser(raw_path)
    resolved = os.path.realpath(expanded)
    home = os.path.realpath(os.path.expanduser("~"))

    if not resolved.startswith(home):
        return jsonify({"error": "Cannot browse outside home directory"}), 403

    if not os.path.isdir(resolved):
        return jsonify({"error": "Not a directory"}), 400

    dirs = []
    try:
        for entry in sorted(os.scandir(resolved), key=lambda e: e.name.lower()):
            if entry.is_dir() and not entry.name.startswith("."):
                dirs.append(entry.name)
    except PermissionError:
        return jsonify({"error": "Permission denied"}), 403

    # Build a display path using ~ prefix when inside home
    if resolved == home:
        display = "~"
    elif resolved.startswith(home + "/"):
        display = "~/" + resolved[len(home) + 1:]
    else:
        display = resolved

    parent = None
    parent_real = os.path.dirname(resolved)
    if parent_real.startswith(home) and resolved != home:
        if parent_real == home:
            parent = "~"
        else:
            parent = "~/" + parent_real[len(home) + 1:]

    return jsonify({
        "path": display,
        "resolved": resolved,
        "parent": parent,
        "dirs": dirs,
    })


@app.route("/api/mkdir", methods=["POST"])
def make_directory():
    data = request.get_json(silent=True) or {}
    raw_parent = (data.get("parent") or "~").strip()
    name = (data.get("name") or "").strip()

    if not name:
        return jsonify({"error": "No folder name provided"}), 400

    if "/" in name or "\\" in name or name.startswith("."):
        return jsonify({"error": "Invalid folder name"}), 400

    parent = os.path.realpath(os.path.expanduser(raw_parent))
    home = os.path.realpath(os.path.expanduser("~"))
    if not parent.startswith(home):
        return jsonify({"error": "Cannot create folders outside home directory"}), 403

    target = os.path.join(parent, name)
    if os.path.exists(target):
        return jsonify({"error": "Folder already exists"}), 400

    try:
        os.makedirs(target)
    except OSError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({"ok": True})


@app.route("/api/export-state")
def export_state():
    home = os.path.realpath(os.path.expanduser("~"))
    idle = ("done", "error", "cancelled", "paused")
    export = {}
    for tid, t in tasks.items():
        if t["status"] not in idle:
            continue
        # Convert download_path to ~-relative for portability
        dp = t.get("download_path", "")
        if dp == home:
            dp = "~"
        elif dp.startswith(home + "/"):
            dp = "~/" + dp[len(home) + 1:]
        export[tid] = {
            "status": t["status"],
            "playlist_title": t.get("playlist_title", ""),
            "videos": t.get("videos", []),
            "video_indices": t.get("video_indices", []),
            "progress": t.get("progress", {}),
            "url": t.get("url", ""),
            "format": t.get("format", "video"),
            "download_path": dp,
            "error": t.get("error"),
        }
    payload = json.dumps({"version": 1, "tasks": export}, indent=2)
    return Response(
        payload,
        mimetype="application/json",
        headers={"Content-Disposition": "attachment; filename=yt-dl-state.json"},
    )


@app.route("/api/import-state", methods=["POST"])
def import_state():
    f = request.files.get("file")
    if f is None:
        return jsonify({"error": "No file provided"}), 400
    try:
        data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return jsonify({"error": "Invalid JSON file"}), 400

    if not isinstance(data, dict) or data.get("version") != 1:
        return jsonify({"error": "Unsupported state file format"}), 400

    idle = ("done", "error", "cancelled", "paused")
    imported = 0
    for _old_id, t in (data.get("tasks") or {}).items():
        if not isinstance(t, dict):
            continue
        status = t.get("status", "")
        if status not in idle:
            continue
        dp = _safe_download_path(t.get("download_path", "~/Downloads"))
        if dp is None:
            continue

        new_id = uuid.uuid4().hex[:12]
        # Reconstruct progress with int keys (JSON keys are strings)
        raw_progress = t.get("progress", {})
        progress = {int(k): v for k, v in raw_progress.items()}

        tasks[new_id] = {
            "status": status,
            "playlist_title": t.get("playlist_title", "Unknown Playlist"),
            "videos": t.get("videos", []),
            "video_indices": t.get("video_indices", []),
            "progress": progress,
            "cancelled": False,
            "paused": False,
            "error": t.get("error"),
            "url": t.get("url", ""),
            "format": t.get("format", "video"),
            "download_path": dp,
        }
        imported += 1

    return jsonify({"ok": True, "imported": imported})


@app.route("/api/tasks")
def list_tasks():
    result = []
    for tid, t in tasks.items():
        total = len(t["video_indices"])
        completed = sum(1 for idx in t["video_indices"] if t["progress"].get(idx, 0) >= 100)
        failed = sum(1 for idx in t["video_indices"] if t["progress"].get(idx, 0) < 0)
        result.append({
            "task_id": tid,
            "playlist_title": t.get("playlist_title", "Unknown"),
            "status": t["status"],
            "url": t.get("url", ""),
            "total": total,
            "completed": completed,
            "failed": failed,
        })
    return jsonify(result)


class _DownloadCancelled(Exception):
    pass


class _DownloadPaused(Exception):
    pass


def _download_one_video(task, idx, video_url, fmt, download_path):
    """Download a single video. Updates task['progress'][idx]."""
    if task["cancelled"] or task["paused"]:
        return

    # Skip if file already exists
    title = ""
    videos = task.get("videos", [])
    if idx < len(videos):
        title = videos[idx].get("title", "")
    if title:
        sanitized = yt_dlp.utils.sanitize_filename(title)
        ext = "mp3" if fmt == "audio" else "mp4"
        expected_file = os.path.join(download_path, f"{sanitized}.{ext}")
        if os.path.isfile(expected_file):
            task["progress"][idx] = 100.0
            return

    task["progress"][idx] = 0.0

    def hook(d):
        if task["cancelled"]:
            raise _DownloadCancelled()
        if task["paused"]:
            raise _DownloadPaused()
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            downloaded = d.get("downloaded_bytes", 0)
            if total > 0:
                task["progress"][idx] = round(downloaded / total * 100, 1)
        elif d.get("status") == "finished":
            task["progress"][idx] = 100.0

    if fmt == "audio":
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": os.path.join(download_path, "%(title)s.%(ext)s"),
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
            "progress_hooks": [hook],
            "quiet": True,
            "no_warnings": True,
        }
    else:
        ydl_opts = {
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "outtmpl": os.path.join(download_path, "%(title)s.%(ext)s"),
            "merge_output_format": "mp4",
            "progress_hooks": [hook],
            "quiet": True,
            "no_warnings": True,
        }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        task["progress"][idx] = 100.0
    except _DownloadCancelled:
        pass  # cancelled, don't mark as failed
    except _DownloadPaused:
        pass  # paused, leave progress as-is
    except Exception:
        task["progress"][idx] = -1


def _retry_videos(task, retry_indices):
    """Retry specific failed videos within an existing task."""
    fmt = task["format"]
    download_path = task["download_path"]
    videos = task["videos"]

    os.makedirs(download_path, exist_ok=True)

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {}
        for idx in retry_indices:
            if task["cancelled"] or task["paused"]:
                break
            entry = videos[idx] if idx < len(videos) else {}
            video_url = (
                entry.get("url")
                or entry.get("webpage_url")
                or f"https://www.youtube.com/watch?v={entry.get('id', '')}"
            )
            future = executor.submit(_download_one_video, task, idx, video_url, fmt, download_path)
            futures[future] = idx

        for future in as_completed(futures):
            if task["cancelled"] or task["paused"]:
                executor.shutdown(wait=False, cancel_futures=True)
                break
            try:
                future.result()
            except Exception:
                pass

    if task["paused"]:
        task["status"] = "paused"
    elif task["cancelled"]:
        task["status"] = "cancelled"
    else:
        task["status"] = "done"


def _download_playlist(task_id, url, fmt, download_path, video_indices, all_videos):
    task = tasks[task_id]

    # If the caller already sent the video list, use it; otherwise fetch.
    if all_videos:
        entries = all_videos
    else:
        ydl_opts_info = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
                info = ydl.extract_info(url, download=False)
            entries = [
                {"id": e.get("id", ""), "title": e.get("title", "Unknown"),
                 "url": e.get("url") or e.get("webpage_url") or ""}
                for e in (info.get("entries") or []) if e is not None
            ]
        except Exception as e:
            task["status"] = "error"
            task["error"] = str(e)
            return

    if not entries:
        task["status"] = "error"
        task["error"] = "No videos found in playlist"
        return

    task["videos"] = [{"id": e.get("id", ""), "title": e.get("title", "Unknown")} for e in entries]

    # Determine which indices to download
    if video_indices is not None:
        indices = [i for i in video_indices if 0 <= i < len(entries)]
    else:
        indices = list(range(len(entries)))

    task["video_indices"] = indices

    if not indices:
        task["status"] = "error"
        task["error"] = "No videos selected"
        return

    os.makedirs(download_path, exist_ok=True)
    task["status"] = "downloading"

    # Initialize progress for all selected videos
    for idx in indices:
        task["progress"][idx] = 0.0

    # Download in parallel
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {}
        for idx in indices:
            if task["cancelled"] or task["paused"]:
                break
            entry = entries[idx]
            video_url = (
                entry.get("url")
                or entry.get("webpage_url")
                or f"https://www.youtube.com/watch?v={entry.get('id', '')}"
            )
            future = executor.submit(_download_one_video, task, idx, video_url, fmt, download_path)
            futures[future] = idx

        # Wait for completion, checking cancellation/pause
        for future in as_completed(futures):
            if task["cancelled"] or task["paused"]:
                executor.shutdown(wait=False, cancel_futures=True)
                break
            # Collect any exceptions (already handled inside _download_one_video)
            try:
                future.result()
            except Exception:
                pass

    if task["paused"]:
        task["status"] = "paused"
    elif task["cancelled"]:
        task["status"] = "cancelled"
    else:
        task["status"] = "done"


if __name__ == "__main__":
    app.run(debug=True, port=5000)
