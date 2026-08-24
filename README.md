# ReClip

A self-hosted, open-source video and audio downloader with a clean web UI. Paste links from YouTube, TikTok, Instagram, Twitter/X, and 1000+ other sites — download as MP4 or MP3.

![Python](https://img.shields.io/badge/python-3.8+-blue)
![License](https://img.shields.io/badge/license-MIT-green)

https://github.com/user-attachments/assets/419d3e50-c933-444b-8cab-a9724986ba05

![ReClip MP3 Mode](assets/preview-mp3.png)

## Features

- Download videos from 1000+ supported sites (via [yt-dlp](https://github.com/yt-dlp/yt-dlp))
- MP4 video or MP3 audio extraction
- Quality/resolution picker
- Bulk downloads — paste multiple URLs at once
- Multi-select completed files and move them between groups
- Automatic URL deduplication
- Multi-account MEGA helper with storage quota checks and transfer progress
- Automatic distribution of selected files across MEGA accounts by free space
- Persistent public MEGA links and remote-only library records after local deletion
- Clean, responsive UI — no frameworks, no build step
- Modular Flask backend with persistent download and upload history

## Quick Start

```bash
brew install yt-dlp ffmpeg    # or apt install ffmpeg && pip install yt-dlp
git clone https://github.com/averygan/reclip.git
cd reclip
./reclip.sh
```

Open **http://localhost:8899**.

Or with Docker Compose:

```bash
docker compose up -d --build
```

The image installs the official rclone release because some distribution builds
omit its MEGA backend. When using `./reclip.sh`, a verified copy is installed
under `.tools/rclone` automatically if the system rclone has no MEGA support.

## Usage

1. Paste one or more video URLs into the input box.
2. Choose **Download videos** or **Scrape page**, then select a destination group
   and the maximum number of concurrent downloads.
3. Choose **MP4** or **MP3**.
4. Use the Fetch split button to choose **Fetch only** or **Fetch & download**.
5. Review fetched cards, choose a quality when available, and download individual,
   selected, or all ready items.
6. Browse, filter, multi-select, move between groups, redownload, delete, or send
   completed files to MEGA from the persistent Downloads library.

The previous interface remains available at **http://localhost:8899/legacy**.

## Fetch performance tuning

Metadata fetching uses bounded worker pools for independent URLs, playlist roots,
and direct-media probes. Successful metadata is cached briefly, and simultaneous
requests for the same URL share one extraction. Fetch concurrency can be changed
from the Fetch button; active batches can also be paused/resumed or stopped from
the fetched-items header. These controls are separate from download concurrency:

| Variable | Default | Purpose |
|---|---:|---|
| `RECLIP_FETCH_WORKERS` | `6` | Initial concurrent per-URL metadata extractions |
| `RECLIP_FETCH_MAX_WORKERS` | `16` | Runtime fetch-concurrency ceiling (1–32) |
| `RECLIP_FETCH_EXPAND_WORKERS` | `4` | Concurrent playlist/provider root expansions (1–8) |
| `RECLIP_FETCH_PROBE_WORKERS` | `8` | Global direct-media probe limit (1–32) |
| `RECLIP_FETCH_PROBE_WINDOW` | `4` | Speculative ordered probes per fetched page |
| `RECLIP_FETCH_CACHE_TTL` | `300` | Successful metadata cache lifetime in seconds (`0` disables it) |
| `RECLIP_FETCH_CACHE_SIZE` | `256` | Maximum cached URL entries (1–2048) |

Lower the worker counts if a media host rate-limits parallel metadata requests or
the server has limited memory.

## MEGA Helper

1. Open **Downloads** and click the red **M** button.
2. Add one or more MEGA accounts. ReClip validates each login and reads its live
   storage quota before saving it.
3. Select completed files by clicking their cards in the Downloads list.
4. Choose **Automatic** to spread files across enabled accounts by available
   storage, or choose one account explicitly.
5. Set the destination folder and queue the upload. Progress, speed, ETA, errors
   and cancellation are available in the same panel.
6. After the transfer, ReClip creates and saves a public MEGA link. Use **Open in
   MEGA** from either the transfer row or Downloads card.
7. Deleting a MEGA-backed item removes only its local media file. The card stays
   visible as **MEGA only**, and ReClip does not delete or unlink the remote file.

Account metadata and the dedicated rclone config are persisted in
`<download-dir>/.mega`. Files are written with owner-only permissions. rclone
password obscuring prevents accidental disclosure but is reversible, so do not
expose an unauthenticated ReClip instance to the public internet; put it behind
authentication and HTTPS.

The quota shown by the plugin is the **MEGA storage quota** (`total`, `used`,
`free`). MEGA/rclone does not provide a reliable account download-transfer
allowance to this integration, so the UI does not invent or estimate that value.

MEGA can create a public link only after the remote object exists, so a completed
transfer briefly enters a **creating link** state. Public links contain the
capability needed to access and decrypt their files; treat them as sensitive.

Optional settings:

| Variable | Default | Purpose |
|---|---:|---|
| `RECLIP_MEGA_CONCURRENT` | `2` | Simultaneous MEGA uploads (1–8) |
| `RECLIP_MEGA_SAFETY_MB` | `16` | Free-space buffer reserved per account |
| `RECLIP_MEGA_QUOTA_TTL` | `60` | Quota cache duration in seconds |
| `RECLIP_MEGA_DIR` | `<downloads>/.mega` | Persistent helper state directory |
| `RECLIP_RCLONE` | `rclone` | Path to a MEGA-enabled rclone binary |
| `RECLIP_NO_RCLONE_INSTALL` | unset | Disable local rclone auto-install in `reclip.sh` |

## Supported Sites

Anything [yt-dlp supports](https://github.com/yt-dlp/yt-dlp/blob/master/supportedsites.md), including:

YouTube, TikTok, Instagram, Twitter/X, Reddit, Facebook, Vimeo, Twitch, Dailymotion, SoundCloud, Loom, Streamable, Pinterest, Tumblr, Threads, LinkedIn, and many more.

## Stack

- **Backend:** Python + Flask
- **Frontend:** Vanilla HTML/CSS/JS templates (no framework or build step)
- **Download engine:** [yt-dlp](https://github.com/yt-dlp/yt-dlp) + [ffmpeg](https://ffmpeg.org/)
- **MEGA engine:** [rclone](https://rclone.org/mega/)
- **Python dependencies:** 2 (Flask, yt-dlp)

## Disclaimer

This tool is intended for personal use only. Please respect copyright laws and the terms of service of the platforms you download from. The developers are not responsible for any misuse of this tool.

## License

[MIT](LICENSE)
