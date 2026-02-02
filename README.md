# Simple YouTube Playlist Downloader

A web-based tool to download YouTube playlists as video (MP4) or audio (MP3). Select specific videos, download in parallel, and manage downloads with pause/resume/cancel controls.

## Features

- Fetch and preview playlist info (title, video count, thumbnails)
- Select individual videos or ranges to download
- Download as MP4 video or MP3 audio (192kbps)
- Parallel downloads (3 concurrent by default)
- Pause, resume, cancel, and retry failed downloads
- Save/load download sessions
- Built-in directory browser for choosing download location

## Requirements

- Python 3.7+
- FFmpeg
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv run app.py
```

Open `http://localhost:5000` in your browser.

## Usage

1. Paste a YouTube playlist URL and click **Fetch Info**
2. Select the videos you want
3. Choose format (Video/Audio) and download location
4. Click **Download Selected**
