import copy
import os
import uuid
import glob
import json
import re
from html import unescape as html_unescape
import signal
import socket
import ipaddress
import shutil
import subprocess
import threading
import time
import sqlite3
from collections import OrderedDict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from urllib.parse import quote, urljoin, urlparse
from urllib.request import Request, urlopen
from flask import Flask, request, jsonify, send_file, render_template
from mega_helper import MegaHelper, MegaHelperError, is_mega_public_url

app = Flask(__name__)
# Files are downloaded to and kept on the server here (override with
# RECLIP_DOWNLOAD_DIR). This app downloads to the server, not the browser.
DOWNLOAD_DIR = os.environ.get("RECLIP_DOWNLOAD_DIR") or os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Bounded downloads: a batch of any size can be submitted, but only
# `gate.limit` yt-dlp processes run at once; the rest wait as "queued".
# The pool is sized to a hard ceiling; the live limit is enforced by the gate
# so the user can change it on the fly without recreating the pool.
MAX_POOL = int(os.environ.get("RECLIP_MAX_POOL", "8"))
DOWNLOAD_TIMEOUT = int(os.environ.get("RECLIP_DOWNLOAD_TIMEOUT", "1800"))
download_pool = ThreadPoolExecutor(max_workers=MAX_POOL)


def _bounded_env_int(name, default, minimum, maximum):
    """Read an integer setting without letting a bad value break startup."""
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, min(value, maximum))


def _bounded_env_float(name, default, minimum, maximum):
    """Read a floating-point setting clamped to a safe range."""
    try:
        value = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        value = float(default)
    return max(minimum, min(value, maximum))


# Metadata extraction is network/process-bound. Keep each stage independently
# bounded so large batches gain parallelism without creating unbounded curl or
# yt-dlp subprocesses. Expansion has its own pool because /api/fetch waits for
# playlist roots; probe work is shared globally by all resolver/scrape jobs.
FETCH_MAX_WORKERS = _bounded_env_int("RECLIP_FETCH_MAX_WORKERS", 16, 1, 32)
FETCH_WORKERS = _bounded_env_int(
    "RECLIP_FETCH_WORKERS", 6, 1, FETCH_MAX_WORKERS
)
FETCH_EXPAND_WORKERS = _bounded_env_int(
    "RECLIP_FETCH_EXPAND_WORKERS", min(4, FETCH_WORKERS), 1, 8
)
FETCH_PROBE_WORKERS = _bounded_env_int("RECLIP_FETCH_PROBE_WORKERS", 8, 1, 32)
FETCH_PROBE_WINDOW = _bounded_env_int(
    "RECLIP_FETCH_PROBE_WINDOW", 4, 1, FETCH_PROBE_WORKERS
)
FETCH_INFO_CACHE_TTL = _bounded_env_float(
    "RECLIP_FETCH_CACHE_TTL", 300, 0, 3600
)
FETCH_INFO_CACHE_SIZE = _bounded_env_int(
    "RECLIP_FETCH_CACHE_SIZE", 256, 1, 2048
)

_fetch_executor = ThreadPoolExecutor(
    max_workers=FETCH_MAX_WORKERS, thread_name_prefix="fetch-info"
)
_fetch_expand_executor = ThreadPoolExecutor(
    max_workers=FETCH_EXPAND_WORKERS, thread_name_prefix="fetch-expand"
)
# Provider expansion may recurse from a root worker into an album frontier.
# A distinct bounded pool avoids nested-submit deadlocks.
_fetch_expand_page_executor = ThreadPoolExecutor(
    max_workers=FETCH_EXPAND_WORKERS, thread_name_prefix="fetch-expand-page"
)
_fetch_probe_executor = ThreadPoolExecutor(
    max_workers=FETCH_PROBE_WORKERS, thread_name_prefix="fetch-probe"
)


class DownloadGate:
    """A resizable, cancellation-aware concurrency limiter.

    Wraps the condition variable, the live-adjustable limit, and the active
    counter in one place. It is essentially a semaphore whose permit count can
    change at runtime and whose waiters can bail out when their job is stopped
    — neither of which threading.Semaphore supports.
    """

    def __init__(self, limit, ceiling):
        self._cond = threading.Condition()
        self._active = 0
        self._ceiling = ceiling
        self._limit = max(1, min(int(limit), ceiling))

    @property
    def limit(self):
        return self._limit

    def acquire(self, job):
        """Block until a slot is free. Returns False if the job is cancelled first."""
        with self._cond:
            while self._active >= self._limit and not job.get("cancelled"):
                self._cond.wait()
            if job.get("cancelled"):
                return False
            self._active += 1
            return True

    def release(self):
        with self._cond:
            self._active = max(0, self._active - 1)
            self._cond.notify_all()

    def set_limit(self, n):
        """Change how many downloads may run at once (clamped to 1..ceiling)."""
        with self._cond:
            self._limit = max(1, min(int(n), self._ceiling))
            self._cond.notify_all()  # let newly-allowed downloads start
        return self._limit

    def wake_all(self):
        """Wake every waiter so a just-cancelled job can observe its flag and bail."""
        with self._cond:
            self._cond.notify_all()


class FetchGate:
    """Resizable scheduler for pausable, stoppable metadata batches."""

    def __init__(self, limit, ceiling):
        self._cond = threading.Condition()
        self._active = 0
        self._ceiling = ceiling
        self._limit = max(1, min(int(limit), ceiling))

    @property
    def limit(self):
        with self._cond:
            return self._limit

    @property
    def active(self):
        with self._cond:
            return self._active

    def acquire(self, batch_id):
        """Wait for global capacity and this batch's pause flag."""
        with self._cond:
            while True:
                with fetch_lock:
                    batch = fetch_batches.get(batch_id)
                    if not batch or batch.get("stopped") or batch.get("finished"):
                        return False
                    paused = bool(batch.get("paused"))
                if not paused and self._active < self._limit:
                    self._active += 1
                    return True
                self._cond.wait()

    def release(self):
        with self._cond:
            self._active = max(0, self._active - 1)
            self._cond.notify_all()

    def set_limit(self, value):
        with self._cond:
            self._limit = max(1, min(int(value), self._ceiling))
            self._cond.notify_all()
            return self._limit

    def wake_all(self):
        with self._cond:
            self._cond.notify_all()


gate = DownloadGate(os.environ.get("RECLIP_MAX_CONCURRENT", "3"), MAX_POOL)
fetch_gate = FetchGate(FETCH_WORKERS, FETCH_MAX_WORKERS)

# Cap how many finished job records we keep in memory (the file itself stays on
# disk). Prevents the in-memory dict from growing without bound on a
# long-running server.
MAX_JOBS = int(os.environ.get("RECLIP_MAX_JOBS", "500"))

jobs = {}
batches = {}
rename_lock = threading.Lock()
# Serializes operations that change a completed library file's path/identity.
# MEGA enqueue uses the same guard so it can never retain a path midway through
# a group move or local deletion.
library_mutation_lock = threading.RLock()

# Dedup: skip downloading a video that's already in DOWNLOAD_DIR.
DEDUP_ENABLED = os.environ.get("RECLIP_DEDUP", "1") not in ("0", "false", "False", "")
INDEX_PATH = os.path.join(DOWNLOAD_DIR, ".reclip_index.json")
download_index = {}
index_lock = threading.Lock()

# yt-dlp prints one progress line per tick using this template (wired up in
# build_ytdlp_command). The sentinel lets us tell progress lines apart from
# yt-dlp's other log output when we stream stdout.
PROGRESS_SENTINEL = "RECLIP_PROGRESS"
# yt-dlp emits the whole progress dict as JSON per tick, so we read fields by
# name instead of by fragile positional order in a pipe-delimited string.
PROGRESS_TEMPLATE = PROGRESS_SENTINEL + "%(progress)j"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
POSTPROCESS_MARKERS = (
    "[Merger]", "[ExtractAudio]", "[VideoConvertor]", "[Fixup",
    "[Metadata]", "Merging formats", "Extracting audio", "Deleting original",
)
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
# Some embed-host CDNs append their streaming token *after* a trailing slash
# on the extension — ``…_720p.mp4/?v-acctoken=…`` — so a bare ``.mp4`` match
# must be allowed to continue past the slash: that per-client access-token
# query is what makes the URL actually stream (without it the host answers
# 403 text/html). Both the absolute and relative forms must accept the same
# optional ``/…`` segment.
MEDIA_URL_RE = re.compile(
    r"https?://[^\s\"'<>]+(?:\.m3u8|\.mp4)(?:/[^\s\"'<>]*)?(?:\?[^\s\"'<>]*)?",
    re.IGNORECASE,
)
REL_MEDIA_RE = re.compile(
    r"(?P<url>/[^\s\"'<>]+(?:\.m3u8|\.mp4)(?:/[^\s\"'<>]*)?(?:\?[^\s\"'<>]*)?)",
    re.IGNORECASE,
)
IFRAME_RE = re.compile(r"<iframe[^>]+src=['\"](?P<src>[^'\"]+)['\"]", re.IGNORECASE)
TITLE_RE = re.compile(r"<title[^>]*>(?P<title>.*?)</title>", re.IGNORECASE | re.DOTALL)
OG_TITLE_RE = re.compile(
    r"<meta[^>]+property=['\"]og:title['\"][^>]+content=['\"](?P<value>[^'\"]+)['\"]",
    re.IGNORECASE,
)
OG_IMAGE_RE = re.compile(
    r"<meta[^>]+property=['\"]og:image['\"][^>]+content=['\"](?P<value>[^'\"]+)['\"]",
    re.IGNORECASE,
)
H1_RE = re.compile(r"<h1[^>]*>(?P<value>.*?)</h1>", re.IGNORECASE | re.DOTALL)


def strip_html(text):
    return re.sub(r"<[^>]+>", "", text or "").strip()


def is_safe_public_url(url):
    """Reject anything that isn't http(s) to a public host.

    The embedded-media resolver fetches user-supplied URLs from the server, so
    without this a caller could point us at cloud metadata endpoints, internal
    services, or file:// paths (SSRF). We resolve the host and refuse any
    private/loopback/link-local/reserved address.
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return False
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(parsed.hostname, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            return False
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_multicast or ip.is_unspecified):
            return False
    return True


def fetch_html(url, referer=None, timeout=20):
    """Fetch a page as text over curl (falling back to urllib).

    Preferring curl matters for Cloudflare-fronted sites whose streaming tokens
    are bound to the fetching client: such hosts mint a short-lived access
    token on the page GET that only works for a matching TLS fingerprint, so
    the page and the later media fetch must use the same client (curl). Falls
    back to urllib if curl is missing or fails, returning None for anything
    non-HTML.
    """
    if not is_safe_public_url(url):
        return None

    cmd = ["curl", "-sS", "-L", "--compressed", "--max-time", str(timeout),
           "-A", USER_AGENT, "--write-out", "\n%{content_type}"]
    if referer:
        cmd += ["-e", referer]
    cmd.append(url)
    try:
        out = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
        if out.returncode != 0:
            raise OSError(out.stderr.decode("utf-8", "replace").strip() or "curl failed")
        body, sep, content_type = out.stdout.rpartition(b"\n")
        content_type = content_type.decode("utf-8", "replace").lower()
    except Exception:
        # curl unavailable or errored — fall back to the urllib path.
        try:
            headers = {"User-Agent": USER_AGENT}
            if referer:
                headers["Referer"] = referer
            with urlopen(Request(url, headers=headers), timeout=timeout) as resp:
                content_type = resp.headers.get("Content-Type", "").lower()
                if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                    return None
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.read().decode(charset, "replace")
        except Exception:
            return None

    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
        return None
    return body.decode("utf-8", "replace")


def extract_page_metadata(html, url):
    thumbnail = None
    title = None

    for regex in (OG_TITLE_RE, TITLE_RE, H1_RE):
        match = regex.search(html)
        if match:
            title = strip_html(match.groupdict().get("value") or match.groupdict().get("title"))
            if title:
                break

    match = OG_IMAGE_RE.search(html)
    if match:
        thumbnail = urljoin(url, match.group("value"))

    return {"title": title or "", "thumbnail": thumbnail or ""}


def find_media_candidates(html, base_url):
    candidates = []

    for match in MEDIA_URL_RE.finditer(html):
        candidates.append(match.group(0))

    for match in REL_MEDIA_RE.finditer(html):
        candidates.append(urljoin(base_url, match.group("url")))

    # Attribute/JS keys. Some players' inline configs declare the real stream
    # as ``video_url: 'https://…/get_file/…/720p.mp4/?<token>'``, so
    # ``video_url`` is an extra key; the extension check also accepts a slash
    # before the query to keep that token (``.mp4/?``) instead of clipping it.
    for needle in ("file", "src", "video_url"):
        pattern = re.compile(
            rf"{needle}\s*[:=]\s*['\"](?P<url>[^'\"]+)['\"]",
            re.IGNORECASE,
        )
        for match in pattern.finditer(html):
            candidate = urljoin(base_url, match.group("url"))
            if re.search(r"\.(m3u8|mp4)(?:[/?#]|$)", candidate, re.IGNORECASE):
                candidates.append(candidate)

    # <meta property="og:video(?:url)?" content="...">
    og_video_re = re.compile(
        r"<meta[^>]+property=['\"]og:video(?:url)?['\"]"
        r"[^>]+content=['\"](?P<url>[^'\"]+)['\"]",
        re.IGNORECASE,
    )
    for match in og_video_re.finditer(html):
        candidates.append(urljoin(base_url, match.group("url")))

    # <source src="..."> inside <video> blocks
    for match in re.finditer(
        r"<source[^>]+src=['\"](?P<url>[^'\"]+)['\"]", html, re.IGNORECASE
    ):
        candidates.append(urljoin(base_url, match.group("url")))

    # data-src / data-source / data-video attributes (lazy-loading players)
    for match in re.finditer(
        r"data-(?:src|source|video)\s*=\s*['\"](?P<url>[^'\"]+)['\"]",
        html, re.IGNORECASE,
    ):
        candidates.append(urljoin(base_url, match.group("url")))

    # A literal <video src="…"> element whose src is the playable/downloadable
    # stream. Token-gated embed CDNs render one like
    #   <video src="https://…/get_file/…/720p.mp4/?<access-token>&amp;rnd=…">
    # Only keep http(s) values (skip blob:/data: players and relative stubs).
    for match in re.finditer(
        r"<video\b[^>]*\bsrc\s*=\s*(['\"])(?P<url>.*?)\1",
        html, re.IGNORECASE | re.DOTALL,
    ):
        src = match.group("url").strip()
        if src.lower().startswith(("http://", "https://")):
            candidates.append(src)

    # HTML attributes are entity-escaped (&amp; → &), so decode every candidate
    # before deduping — tokens like ``?v-acctoken=…&amp;rnd=…`` must come back out
    # as a working URL, not ``&amp;``.
    seen = set()
    deduped = []
    for raw in candidates:
        candidate = html_unescape(raw)
        if candidate not in seen:
            deduped.append(candidate)
            seen.add(candidate)
    return deduped


def _fmt_bytes(n):
    """Human-readable byte count, yt-dlp style ('12.3 MiB')."""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return ""
    if not n or n < 0 or n != n:
        return ""
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f"{n:.0f} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return ""


MAX_CANDIDATE_PROBES = 8  # try at most N scraped candidates before falling back


def media_verdict(url, referer=None):
    """Classify a scraped media candidate: 1 = real stream, 0 = dead placeholder,
    -1 = indeterminate (transient errors like CDN 401/403/429 or network blips).

    Probes with a tiny ranged curl request: Cloudflare-fronted CDN endpoints
    403 urllib/requests TLS fingerprints but serve curl fine.
    On some hosts the bare ``…<id>.mp4`` path answers a tiny GIF placeholder
    (image/gif) while a quality-suffixed sibling (``…<id>_720p.mp4``) is the
    real stream, so the probe's final content-type (after the CDN redirect)
    tells them apart. -1 must not make the
    resolver give up on the page's own candidate and grab an unrelated video.
    """
    cmd = ["curl", "-sS", "-L", "--max-time", "15", "-o", "/dev/null",
           "-w", "%{http_code}|%{content_type}", "-A", USER_AGENT,
           "-H", "Range: bytes=0-1023", "-H", "Accept: */*"]
    if referer:
        cmd += ["-e", referer]
    cmd.append(url)
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception:
        return -1

    m = re.match(r"(\d+)\|(.*)", out)
    if not m:
        return -1
    code, ctype = m.group(1), m.group(2).split(";")[0].strip().lower()
    if not ctype or code not in ("200", "206"):
        return -1
    if ctype.startswith("video/"):
        return 1
    if ctype in ("application/vnd.apple.mpegurl", "application/x-mpegurl",
                 "application/mpegurl", "audio/mpegurl"):
        return 1
    if ctype.startswith("image/") or ctype.startswith("text/"):
        return 0
    if ctype == "application/octet-stream":
        # Could be real media behind a generic content-type; peek a few bytes to
        # rule out image/HTML placeholders and confirm HLS playlists.
        head_cmd = ["curl", "-sS", "-L", "--max-time", "15", "-r", "0-63",
                    "-A", USER_AGENT]
        if referer:
            head_cmd += ["-e", referer]
        head_cmd.append(url)
        try:
            head = subprocess.run(head_cmd, capture_output=True, timeout=20).stdout
        except Exception:
            return -1
        if head.startswith(b"#EXTM3U"):
            return 1
        if not head:
            return -1
        return 0 if head.startswith(MEDIA_PLACEHOLDER_MAGIC) else 1
    return -1


def _ordered_media_verdicts(candidates, referer=None):
    """Yield probe results in candidate order while probing ahead concurrently.

    Resolver choice remains deterministic: callers observe exactly the same
    ranked order as the old serial loop. At most FETCH_PROBE_WINDOW requests
    are speculative for one caller, while the shared executor bounds probes
    across every active fetch batch. Pending work is cancelled when a caller
    finds a winner early.
    """
    items = list(candidates)
    pending = {}
    next_index = 0
    initial = min(len(items), FETCH_PROBE_WINDOW)
    for next_index in range(initial):
        pending[next_index] = _fetch_probe_executor.submit(
            media_verdict, items[next_index], referer
        )
    next_index = initial

    try:
        for index, candidate in enumerate(items):
            future = pending.pop(index)
            try:
                verdict = future.result()
            except Exception:
                verdict = -1
            yield candidate, verdict
            if next_index < len(items):
                pending[next_index] = _fetch_probe_executor.submit(
                    media_verdict, items[next_index], referer
                )
                next_index += 1
    finally:
        for future in pending.values():
            future.cancel()


def _candidate_rank(candidate, video_id):
    """Score how likely a scraped URL is the page's own playable stream.

    URLs carrying the page's video id and a resolution suffix (e.g. the 720p
    variant) rank highest; same-id bare files next; anything else (related-video
    previews, screenshots) lowest. Higher is better.
    """
    c = candidate.lower()
    has_id = bool(video_id) and (video_id.lower() in c)
    hd = bool(re.search(r"_\d{2,4}p(?:\.m3u8|\.mp4)", c))
    if has_id and hd:
        return 3
    if has_id:
        return 2
    if hd:
        return 1
    return 0


def _media_total_size(media_url, referer=None):
    """Fetch the byte size of a direct media file via a 1-byte range request."""
    cmd = ["curl", "-sS", "-L", "--max-time", "20", "-r", "0-0", "-D", "-",
           "-o", "/dev/null", "-A", USER_AGENT]
    if referer:
        cmd += ["-e", referer]
    cmd.append(media_url)
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=25).stdout
    except Exception:
        return None
    m = re.search(r"[Cc]ontent-[Rr]ange:\s*bytes\s+\d+-\d+/(\d+)", out)
    if m:
        return int(m.group(1))
    m = re.search(r"[Cc]ontent-[Ll]ength:\s*(\d+)", out)
    if m:
        return int(m.group(1))
    return None


MEDIA_PLACEHOLDER_MAGIC = (b"GIF8", b"\x89PNG", b"\xff\xd8", b"BM",
                           b"RIFF", b"<!DOCTYPE", b"<html")


def _is_media_file(path):
    """True if a downloaded file looks like real media (not an error placeholder)."""
    try:
        size = os.path.getsize(path)
    except OSError:
        return False
    if size < 64:
        return False
    try:
        with open(path, "rb") as fh:
            head = fh.read(64)
    except OSError:
        return False
    return not head.startswith(MEDIA_PLACEHOLDER_MAGIC)


def find_iframe_urls(html, base_url):
    urls = []
    for match in IFRAME_RE.finditer(html):
        src = urljoin(base_url, match.group("src"))
        if src not in urls:
            urls.append(src)
    return urls


# ---- Known embed-host providers -------------------------------------------
# Some video hosts serve a wrapper page whose real stream only exists behind a
# site-specific flow (e.g. a JSON API endpoint) that plain HTML scraping can't
# reach. Each provider knows how to turn one of its embed URLs into a direct
# media URL (plus where to load it from as Referer). Anything in this registry
# is tried *before* generic page scraping, both for the pasted URL itself and
# for every iframe we discover while walking a page.
#
# The registry lives outside the source tree: providers are loaded from
# providers.json next to this file (override with RECLIP_PROVIDERS), so hosts,
# domains and endpoints never need to be hardcoded here. See
# providers.example.json for the schema; a missing or invalid file simply
# leaves the registry empty and generic scraping carries on alone.

PROVIDERS_PATH = (
    os.environ.get("RECLIP_PROVIDERS")
    or os.path.join(os.path.dirname(__file__), "providers.json")
)


def _load_video_providers(path):
    """Load site-specific embed providers from ``path``.

    Each entry needs ``name`` + ``url_pattern`` plus whatever its ``kind``
    requires; anything that doesn't parse is skipped rather than fatal.
    """
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return []
    raw = doc.get("providers") if isinstance(doc, dict) else None
    if not isinstance(raw, list):
        return []
    loaded = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        try:
            compiled = re.compile(entry["url_pattern"], re.IGNORECASE)
        except (KeyError, TypeError, re.error):
            continue
        provider = dict(entry)
        provider["url_re"] = compiled
        loaded.append(provider)
    return loaded


VIDEO_PROVIDERS = _load_video_providers(PROVIDERS_PATH)


def _resolve_json_stream_api(url, prov):
    """Resolve a configured JSON stream-API embed to its direct media URL.

    These embed pages run a JS player that pulls the real source from a JSON
    endpoint keyed by the id embedded in the URL. The embed may redirect to a
    mirror origin; when read, reuse the API base it declares in its own source,
    otherwise fall back to the configured default endpoint.
    """
    match = re.search(prov["filecode_pattern"], url)
    if not match:
        return None

    api_url = None
    html = fetch_html(url, referer=url)
    if html:
        api_match = re.search(prov["api_url_page_regex"], html)
        if api_match:
            base = urlparse(url)
            api_url = urljoin(f"{base.scheme}://{base.netloc}", api_match.group(1))
    if not api_url:
        api_url = prov["api_url_fallback"]

    payload = dict(prov.get("payload") or {})
    payload[prov.get("id_key") or "filecode"] = match.group(1)
    req = Request(api_url, data=json.dumps(payload).encode(), headers={
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Referer": url,
    })
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8", "replace"))
    data = data or {}

    fields = prov.get("fields") or {}
    media_url = data.get(fields.get("media_url") or "media_url")
    if not media_url:
        return None
    return {
        "media_url": media_url,
        "referer": url,
        "title": data.get(fields.get("title") or "title") or "",
        "thumbnail": data.get(fields.get("thumbnail") or "thumbnail") or "",
        "extractor": prov.get("name") or "",
    }


def _resolve_signed_file(url, prov):
    """Resolve a configured signed-file-host URL to its direct media URL.

    These file hosts wrap every asset behind three hops: a listing page
    linking to a per-file download page, that page serving a JS
    "fetch-metadata-then-sign" flow instead of the actual bytes, and a signing
    service that mints a short-lived token for the raw CDN path. Replaying
    those calls server-side yields a ready-to-stream URL plus the real
    filename (used as the title). Tokens expire quickly, so this is called at
    download time, never cached.
    """
    # A pasted URL can be the download-page link itself — skip straight to it.
    direct = re.match(prov["direct_dl_pattern"], url) if prov.get("direct_dl_pattern") else None
    title = thumbnail = ""
    if direct:
        dl_url = url
    else:
        html = fetch_html(url)
        if not html:
            return None
        dl_match = re.search(prov["dl_link_regex"], html)
        if not dl_match:
            return None
        dl_url = dl_match.group(0)
        meta0 = extract_page_metadata(html, url)
        title, thumbnail = meta0.get("title") or "", meta0.get("thumbnail") or ""

    dl_html = fetch_html(dl_url)
    if not dl_html:
        return None
    id_match = re.search(prov["data_id_regex"], dl_html)
    if not id_match:
        return None

    req = Request(prov["meta_api"], data=json.dumps({"id": id_match.group(1)}).encode(),
                  headers={"User-Agent": USER_AGENT,
                           "Content-Type": "application/json",
                           "Referer": dl_url})
    with urlopen(req, timeout=30) as resp:
        meta = json.loads(resp.read().decode("utf-8", "replace")) or {}
    mediafiles, path, original = (meta.get("mediafiles") or "",
                                  meta.get("path") or "",
                                  meta.get("original") or "")
    if not (mediafiles and path):
        return None

    sign_url = prov["sign_service"] + "?path=" + quote(path, safe="")
    with urlopen(Request(sign_url, headers={"User-Agent": USER_AGENT}), timeout=30) as resp:
        sig = json.loads(resp.read().decode("utf-8", "replace")) or {}
    token, ex = sig.get("token"), sig.get("ex")
    if not token or not ex:
        return None

    media_url = (f"{mediafiles}{path}?n={quote(original)}"
                 f"&token={token}&ex={ex}") if original else \
                (f"{mediafiles}{path}?token={token}&ex={ex}")
    parsed = urlparse(mediafiles)
    return {
        "media_url": media_url,
        "referer": f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else None,
        "title": original or title,
        "thumbnail": thumbnail,
        "extractor": prov.get("name") or "",
        # A non-empty filename marks this as a *fixed file* (archive, image,
        # clip) rather than an adaptive stream — run_download routes those to
        # the plain HTTP downloader instead of yt-dlp.
        "filename": original or "",
    }


# Dispatch table: provider ``kind`` -> resolver callable(url, provider).
_PROVIDER_RESOLVERS = {
    "json_stream_api": _resolve_json_stream_api,
    "signed_file_host": _resolve_signed_file,
}


def _provider_resolver_applies(url):
    """True when ``url`` belongs to a provider with a direct resolver."""
    for provider in VIDEO_PROVIDERS:
        if provider.get("kind") in _PROVIDER_RESOLVERS and provider["url_re"].match(url):
            return True
    return False


def _expand_links_page(url, prov):
    """Return the absolute item links one listing/album page contains."""
    html = fetch_html(url)
    if not html:
        return []
    item_pat = re.compile(prov["item_href_regex"], re.IGNORECASE)
    items, seen = [], set()
    for m in re.finditer(r"href\s*=\s*['\"]([^'\"]+)['\"]", html, re.IGNORECASE):
        full = urljoin(url, m.group(1))
        if full in seen or not item_pat.search(full):
            continue
        seen.add(full)
        items.append(full)
        if len(items) >= int(prov.get("max_items") or 100):
            break
    return items


# Dispatch table: provider ``kind`` -> expander callable(url, provider).
_PROVIDER_EXPANDERS = {
    "link_list_page": _expand_links_page,
}


def _matching_expander(url):
    for provider in VIDEO_PROVIDERS:
        if provider.get("kind") in _PROVIDER_EXPANDERS and provider["url_re"].match(url):
            return provider
    return None


# Bounds for provider-driven expansion: an index page can fan out into many
# albums, each album into many files — cap both the walk depth and the total
# number of leaf URLs one paste may produce.
MAX_EXPAND_DEPTH = 3
EXPAND_TOTAL_LIMIT = 120


def _expand_via_providers(url):
    """Expand provider listing pages recursively (index → albums → files).

    Returns None when ``url`` matches no expander (the caller then falls back
    to yt-dlp playlist handling); otherwise the flat, deduplicated list of
    leaf URLs, capped at EXPAND_TOTAL_LIMIT. An expander that errors or finds
    nothing leaves the URL itself in place so the user still gets a visible
    (erroring) card instead of a silently vanished one.
    """
    if _matching_expander(url) is None:
        return None
    results, queue, seen, depth = [], [url], {url}, 0
    while queue and depth < MAX_EXPAND_DEPTH:
        nxt = []
        # A listing can fan out to many independent albums. Fetch one frontier
        # concurrently, then consume results in queue order so deduplication and
        # output order remain deterministic.
        pending = {}
        providers = {}
        for index, item_url in enumerate(queue):
            provider = _matching_expander(item_url)
            providers[index] = provider
            if provider is not None:
                pending[index] = _fetch_expand_page_executor.submit(
                    _PROVIDER_EXPANDERS[provider["kind"]], item_url, provider
                )

        for index, item_url in enumerate(queue):
            provider = providers[index]
            if provider is None:
                results.append(item_url)
                continue
            try:
                children = pending[index].result()
            except Exception:
                children = []
            for child in children:
                if child not in seen:
                    seen.add(child)
                    nxt.append(child)
        queue = nxt
        depth += 1
        room = EXPAND_TOTAL_LIMIT - len(results)
        if room <= 0:
            queue = []
        elif len(queue) > room:
            queue = queue[:room]
    results.extend(queue)
    return results or None


def resolve_provider(url):
    """Try the registered embed-host providers against ``url``.

    Returns the first resolved dict (media_url/referer/title/thumbnail) whose
    provider owns the URL, or None when no provider applies or fails.
    """
    if not is_safe_public_url(url):
        return None
    for provider in VIDEO_PROVIDERS:
        if not provider["url_re"].match(url):
            continue
        resolver = _PROVIDER_RESOLVERS.get(provider.get("kind"))
        if resolver is None:
            continue
        try:
            resolved = resolver(url, provider)
        except Exception:
            resolved = None
        if resolved:
            return resolved
    return None


# Hard cap on total pages fetched while resolving one URL, so a page that fans
# out into many iframes can't tie up a request for minutes.
MAX_RESOLVE_FETCHES = 6


def resolve_embedded_media(url, referer=None, visited=None, depth=0, budget=None):
    if visited is None:
        visited = set()
    if budget is None:
        budget = {"left": MAX_RESOLVE_FETCHES}
    if depth > 3 or url in visited or budget["left"] <= 0:
        return None
    visited.add(url)
    budget["left"] -= 1

    # 1) A configured embed-host provider exposes the real stream behind
    #    a site-specific flow — plain page scraping below can't reach it, so
    #    check the registry first. This also catches iframes further down the
    #    walk, since we recurse into resolve_embedded_media on each iframe URL.
    provider_resolved = resolve_provider(url)
    if provider_resolved:
        return provider_resolved

    html = fetch_html(url, referer=referer)
    if not html:
        return None

    metadata = extract_page_metadata(html, url)
    media_candidates = find_media_candidates(html, url)
    if media_candidates:
        # The page's first scraped source can be a dead server-side URL
        # (some hosts answer a tiny GIF placeholder for their bare mp4)
        # while a sibling ``…_720p.mp4`` variant is the real stream. Score candidates so the
        # page's own video (same id, HD suffix) wins over related-video previews,
        # then pick the first one that actually delivers media. A transient
        # probe failure (verdict -1) never demotes the page's own candidate to a
        # different video.
        video_id = None
        m = re.search(r"/video/(\d+)", url)
        if m:
            video_id = m.group(1)
        ranked = sorted(media_candidates[:MAX_CANDIDATE_PROBES],
                        key=lambda c: -_candidate_rank(c, video_id))
        chosen = None
        verdicts = _ordered_media_verdicts(ranked, referer=url)
        try:
            for candidate, verdict in verdicts:
                if verdict == 1:
                    chosen = candidate
                    break
                if verdict == 0:
                    continue
                if _candidate_rank(candidate, video_id) >= 2:
                    chosen = candidate  # indeterminate but it IS this page's file
                    break
        finally:
            verdicts.close()
        if chosen is None:
            chosen = media_candidates[0]  # historic fallback
        return {
            "media_url": chosen,
            "referer": url,
            "title": metadata["title"],
            "thumbnail": metadata["thumbnail"],
        }

    for iframe_url in find_iframe_urls(html, url):
        if budget["left"] <= 0:
            break
        resolved = resolve_embedded_media(
            iframe_url, referer=url, visited=visited, depth=depth + 1, budget=budget
        )
        if resolved:
            if not resolved.get("title"):
                resolved["title"] = metadata["title"]
            if not resolved.get("thumbnail"):
                resolved["thumbnail"] = metadata["thumbnail"]
            return resolved

    return None


def build_ytdlp_command(url, output_template=None, format_choice=None, format_id=None, referer=None, json_mode=False):
    cmd = ["yt-dlp", "--no-playlist"]
    if referer:
        cmd += ["--referer", referer]
    if json_mode:
        cmd.append("-j")
    elif output_template:
        cmd += ["-o", output_template]
        cmd += ["--newline", "--progress-template", PROGRESS_TEMPLATE]
        # HLS/DASH streams are downloaded a few segments at a time; without
        # this yt-dlp serializes every fragment, which makes some CDNs'
        # HLS feeds noticeably slow.
        cmd += ["--concurrent-fragments", "8"]

        if format_choice == "audio":
            cmd += ["-x", "--audio-format", "mp3"]
        elif format_id and format_id != "direct":
            # "direct" is the card-level pseudo-format for pre-resolved single
            # sources — there is no such yt-dlp format id, so fall through to
            # the default selection and just grab whatever the URL serves.
            cmd += ["-f", f"{format_id}+bestaudio/best", "--merge-output-format", "mp4"]
        else:
            cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

    cmd.append(url)
    return cmd


def run_ytdlp_json(url, referer=None, timeout=60):
    cmd = build_ytdlp_command(url, referer=referer, json_mode=True)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip().split("\n")[-1])
    return parse_ytdlp_json(result.stdout)


def parse_ytdlp_json(stdout):
    """Parse yt-dlp JSON output.

    With ``-j`` yt-dlp prints one JSON object per line. Some extractors
    emit multiple videos even with ``--no-playlist``, so stdout contains
    several objects and a plain ``json.loads`` raises "Extra data".
    Return the first valid object.
    """
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        return json.loads(line)
    raise ValueError("yt-dlp returned no data")


def parse_progress_line(line):
    """Return {percent, speed, eta, downloaded, total} for a progress line, else None."""
    line = ANSI_RE.sub("", line).strip()
    if not line.startswith(PROGRESS_SENTINEL):
        return None
    try:
        d = json.loads(line[len(PROGRESS_SENTINEL):].strip())
    except ValueError:
        return None

    def clean(v):
        v = (v or "").strip()
        return "" if v in ("Unknown", "Unknown B/s", "N/A", "NA", "") else v

    downloaded_bytes = d.get("downloaded_bytes")
    total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate")
    percent = None
    if isinstance(downloaded_bytes, (int, float)) and isinstance(total_bytes, (int, float)) and total_bytes > 0:
        percent = round(downloaded_bytes / total_bytes * 100, 1)
    elif isinstance(d.get("_percent"), (int, float)):
        percent = round(d["_percent"], 1)

    return {
        "percent": percent,
        "speed": clean(d.get("_speed_str")),
        "eta": clean(d.get("_eta_str")),
        "downloaded": clean(d.get("_downloaded_bytes_str")),
        "total": clean(d.get("_total_bytes_str")) or clean(d.get("_total_bytes_estimate_str")),
    }


def kill_proc(proc):
    """Hard-kill yt-dlp *and* its ffmpeg children.

    yt-dlp spawns ffmpeg (HLS downloads, the merge step) in the same session.
    proc.kill() would leave those children running — holding the stdout pipe
    open (so our read loop never sees EOF) and writing files we're about to
    clean up — so we signal the whole process group.
    """
    if proc is None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except Exception:
            pass


def run_ytdlp_streaming(job, cmd, timeout):
    """Run a yt-dlp download, streaming progress into ``job``.

    Returns (returncode, tail_lines, timed_out). Progress ticks update
    job["progress"]/["speed"]/["eta"]; post-processing (merge / audio
    extraction) flips the job into the "processing" status. A watchdog
    hard-kills the process group at ``timeout`` even if it stops emitting output.
    """
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
        start_new_session=True,  # own process group, so kill_proc reaches ffmpeg
    )
    job["proc"] = proc
    if job.get("cancelled"):
        # A stop request landed between submit and Popen — kill right away.
        kill_proc(proc)
    tail = deque(maxlen=25)
    killed = {"timeout": False}

    def _kill():
        killed["timeout"] = True
        kill_proc(proc)

    watchdog = threading.Timer(timeout, _kill)
    watchdog.start()
    try:
        for raw in proc.stdout:
            progress = parse_progress_line(raw)
            if progress:
                if progress["percent"] is not None:
                    job["progress"] = progress["percent"]
                job["speed"] = progress["speed"]
                job["eta"] = progress["eta"]
                job["downloaded"] = progress["downloaded"]
                job["total_size"] = progress["total"]
                job["status"] = "downloading"
                continue
            clean = ANSI_RE.sub("", raw).strip()
            if not clean:
                continue
            tail.append(clean)
            if any(marker in clean for marker in POSTPROCESS_MARKERS):
                job["status"] = "processing"
                job["speed"] = ""
                job["eta"] = ""
    except Exception:
        # A read/decode error must not leak the process — kill it before we
        # unwind, otherwise it keeps downloading with the watchdog cancelled.
        kill_proc(proc)
    finally:
        watchdog.cancel()
        try:
            proc.wait()
        except Exception:
            pass
        job["proc"] = None
    return proc.returncode, list(tail), killed["timeout"]


def remove_job_files(job_id, keep=None):
    """Delete a job's DOWNLOAD_DIR/{job_id}.* staging files (optionally keeping one)."""
    keep_abs = os.path.abspath(keep) if keep else None
    for f in glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}.*")):
        if keep_abs and os.path.abspath(f) == keep_abs:
            continue
        try:
            os.remove(f)
        except OSError:
            pass


def pick_output_file(job_id, format_choice):
    """Return the finished file for a job, deleting any stray intermediates."""
    files = glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}.*"))
    if not files:
        return None
    ext = ".mp3" if format_choice == "audio" else ".mp4"
    preferred = [f for f in files if f.endswith(ext)]
    chosen = preferred[0] if preferred else files[0]
    remove_job_files(job_id, keep=chosen)
    return chosen


def _unique_path(base, ext, directory=None):
    directory = directory or DOWNLOAD_DIR
    candidate = os.path.join(directory, f"{base}{ext}")
    if not os.path.exists(candidate):
        return candidate
    i = 2
    while True:
        candidate = os.path.join(directory, f"{base} ({i}){ext}")
        if not os.path.exists(candidate):
            return candidate
        i += 1


def sanitize_title(title):
    """Filesystem-safe version of a video title (also used as a dedup fallback key)."""
    return "".join(c for c in (title or "") if c not in r'\/:*?"<>|').strip()[:100].strip()


# File types treated as already-correct when a human-readable title carries
# its own extension (direct-file CDNs often serve a generic Content-Type, so
# downloads stage as '.unknown_video' and stacking that on the title would
# produce names like 'clip.mp4.unknown_video').
KNOWN_MEDIA_EXTS = {
    ".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi", ".wmv", ".flv",
    ".ts", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac",
    ".zip", ".rar", ".7z", ".tar", ".gz",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".pdf",
}


def store_final_file(job_id, staged_path, title, target_dir=None):
    """Rename the job-id staging file to a human-readable name.

    When ``target_dir`` (a group folder) is given the file is moved there;
    otherwise it stays in the top-level DOWNLOAD_DIR.
    """
    target_dir = target_dir or DOWNLOAD_DIR
    ext = os.path.splitext(staged_path)[1]
    base = sanitize_title(title) or job_id
    t_root, t_ext = os.path.splitext(base)
    if t_ext and t_ext.lower() in KNOWN_MEDIA_EXTS:
        base, ext = t_root, t_ext  # keep the title's own casing
    elif ext and base.lower().endswith(ext.lower()):
        # Title already carries the staged extension (split archives like
        # 'movie.mp4.7z.001', token-named files…) — don't stack it twice.
        ext = ""
    desired = os.path.join(target_dir, f"{base}{ext}")
    if os.path.abspath(desired) == os.path.abspath(staged_path):
        return staged_path
    with rename_lock:
        final_path = _unique_path(base, ext, target_dir)
        try:
            os.rename(staged_path, final_path)
        except OSError:
            return staged_path
    return final_path


# ---- Dedup index -----------------------------------------------------------
# A persisted map of already-downloaded videos so we never fetch the same one
# twice. Keyed primarily by the extractor's canonical id (robust across
# different URLs / title changes / restarts), with a title fallback.
#
# Persistence is SQLite (RECLIP_DB, default <download dir>/reclip.db) so the
# download history survives server restarts and scales well as it grows. A
# legacy .reclip_index.json is imported once on first run against an empty DB.

_db_lock = threading.Lock()
DB_PATH = os.environ.get("RECLIP_DB") or os.path.join(DOWNLOAD_DIR, "reclip.db")
THUMBS_SUBDIR = "thumbs"

_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS downloads (
  key       TEXT PRIMARY KEY,
  file      TEXT NOT NULL,
  filename  TEXT,
  title     TEXT,
  url       TEXT,
  extractor TEXT,
  video_id  TEXT,
  created_at REAL,
  duration   REAL,
  thumb      TEXT,
  group_id   TEXT,
  size_bytes INTEGER,
  local_deleted_at REAL,
  mega_url TEXT,
  mega_remote_path TEXT,
  mega_account_id TEXT,
  mega_account_label TEXT,
  mega_uploaded_at REAL
);
CREATE TABLE IF NOT EXISTS groups (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  created_at REAL
);
CREATE TABLE IF NOT EXISTS fetch_batches (
  id         TEXT PRIMARY KEY,
  urls       TEXT NOT NULL,
  created_at REAL,
  finished   INTEGER,
  kind       TEXT
);
"""


def _db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_db():
    """Create the schema and adopt a legacy .reclip_index.json if the DB is empty."""
    with _db_lock:
        conn = _db_connect()
        try:
            conn.executescript(_DB_SCHEMA)
            cols = [r[1] for r in conn.execute("PRAGMA table_info(downloads)")]
            if "duration" not in cols:
                conn.execute("ALTER TABLE downloads ADD COLUMN duration REAL")
            if "thumb" not in cols:
                conn.execute("ALTER TABLE downloads ADD COLUMN thumb TEXT")
            if "group_id" not in cols:
                conn.execute("ALTER TABLE downloads ADD COLUMN group_id TEXT")
            download_migrations = {
                "size_bytes": "INTEGER",
                "local_deleted_at": "REAL",
                "mega_url": "TEXT",
                "mega_remote_path": "TEXT",
                "mega_account_id": "TEXT",
                "mega_account_label": "TEXT",
                "mega_uploaded_at": "REAL",
            }
            for column, kind in download_migrations.items():
                if column not in cols:
                    conn.execute(f"ALTER TABLE downloads ADD COLUMN {column} {kind}")
            fb_cols = [r[1] for r in conn.execute("PRAGMA table_info(fetch_batches)")]
            if "kind" not in fb_cols:
                conn.execute("ALTER TABLE fetch_batches ADD COLUMN kind TEXT DEFAULT 'video'")
            conn.commit()
            count = conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]
            imported = False
            if count == 0 and os.path.exists(INDEX_PATH):
                try:
                    with open(INDEX_PATH) as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        now = time.time()
                        rows = [(
                            k, e.get("file", ""), e.get("filename"),
                            e.get("title"), e.get("url"),
                            e.get("extractor"), e.get("video_id"), now, None, None, None,
                        ) for k, e in data.items() if e.get("file")]
                        with conn:
                            conn.executemany(
                                "INSERT OR REPLACE INTO downloads "
                                "(key,file,filename,title,url,extractor,video_id,created_at,"
                                "duration,thumb,group_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                rows,
                            )
                        imported = True
                except (OSError, ValueError):
                    pass
            if imported:
                try:
                    os.remove(INDEX_PATH)  # DB is authoritative now
                except OSError:
                    pass
        finally:
            conn.close()


def _row_to_entry(row):
    (
        _key, file_, filename, title, url, extractor, video_id, created_at,
        duration, thumb, group_id, size_bytes, local_deleted_at, mega_url,
        mega_remote_path, mega_account_id, mega_account_label, mega_uploaded_at,
    ) = row
    return {
        "file": file_,
        "filename": filename or os.path.basename(file_),
        "title": title or "",
        "url": url or "",
        "extractor": extractor or "",
        "video_id": video_id,
        "created_at": created_at or None,
        "duration": duration or None,
        "thumb": thumb or None,
        "group_id": group_id or None,
        "size_bytes": size_bytes,
        "local_deleted_at": local_deleted_at or None,
        "mega_url": mega_url or None,
        "mega_remote_path": mega_remote_path or None,
        "mega_account_id": mega_account_id or None,
        "mega_account_label": mega_account_label or None,
        "mega_uploaded_at": mega_uploaded_at or None,
    }


def load_index():
    global download_index
    data = {}
    try:
        with _db_lock:
            conn = _db_connect()
            try:
                for row in conn.execute(
                    "SELECT key,file,filename,title,url,extractor,video_id,created_at,"
                    "duration,thumb,group_id,size_bytes,local_deleted_at,mega_url,"
                    "mega_remote_path,mega_account_id,mega_account_label,mega_uploaded_at "
                    "FROM downloads"):
                    data[row[0]] = _row_to_entry(row)
            finally:
                conn.close()
    except Exception:
        data = {}
    download_index = data


def _save_index_locked(strict=False):
    """Persist the whole dedup index to SQLite (called while holding index_lock)."""
    try:
        with _db_lock:
            conn = _db_connect()
            try:
                now = time.time()
                with conn:
                    conn.execute("DELETE FROM downloads")
                    conn.executemany(
                        "INSERT INTO downloads "
                        "(key,file,filename,title,url,extractor,video_id,created_at,"
                        "duration,thumb,group_id,size_bytes,local_deleted_at,mega_url,"
                        "mega_remote_path,mega_account_id,mega_account_label,mega_uploaded_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        [(k, e.get("file", ""), e.get("filename"), e.get("title"),
                          e.get("url"), e.get("extractor"), e.get("video_id"),
                          e.get("created_at") or now, e.get("duration"), e.get("thumb"),
                          e.get("group_id"), e.get("size_bytes"),
                          e.get("local_deleted_at"), e.get("mega_url"),
                          e.get("mega_remote_path"), e.get("mega_account_id"),
                          e.get("mega_account_label"), e.get("mega_uploaded_at"))
                         for k, e in download_index.items()])
            finally:
                conn.close()
    except Exception:
        if strict:
            raise


# ---- Download groups ------------------------------------------------------
# Groups are named folders under DOWNLOAD_DIR so a batch of videos can be
# organised into separate directories. The group id is a slug that doubles as
# the folder name; renaming only changes the display name (folder stays stable,
# so on-disk file paths never move). Groups are persisted in SQLite.

groups = {}
groups_lock = threading.Lock()


def _load_groups():
    global groups
    g = {}
    try:
        with _db_lock:
            conn = _db_connect()
            try:
                for row in conn.execute("SELECT id, name, created_at FROM groups"):
                    g[row[0]] = {"name": row[1], "created_at": row[2]}
            finally:
                conn.close()
    except Exception:
        g = {}
    groups = g


def _save_groups_locked():
    try:
        with _db_lock:
            conn = _db_connect()
            try:
                now = time.time()
                with conn:
                    conn.execute("DELETE FROM groups")
                    conn.executemany(
                        "INSERT INTO groups VALUES (?,?,?)",
                        [(gid, g["name"], g.get("created_at") or now)
                         for gid, g in groups.items()])
            finally:
                conn.close()
    except Exception:
        pass


def _slugify(name):
    s = re.sub(r"[^A-Za-z0-9]+", "-", (name or "").strip().lower()).strip("-")
    return s or "group"


def group_dir(gid):
    """Return the group's folder path (creating it), or None when unknown."""
    if not gid or gid not in groups:
        return None
    d = os.path.join(DOWNLOAD_DIR, gid)
    os.makedirs(d, exist_ok=True)
    return d


def group_name(gid):
    if not gid:
        return ""
    g = groups.get(gid)
    return g.get("name", "") if g else ""


# Extractors whose video ids are NOT unique per video (they reuse ids like
# "master" for every master.m3u8), so we must not dedup different videos by them.
UNRELIABLE_EXTRACTORS = ("", "generic", "genericie")


def _quality_token(format_choice, format_id):
    """A stable token for the requested output, so different resolutions don't collide."""
    if format_choice == "audio":
        return "audio"
    return f"video:{format_id or 'best'}"


def _dedup_keys(video_id, extractor, url, format_choice, format_id):
    """Keys identifying this exact video+quality.

    Only trust the extractor's id for real extractors (YouTube, Vimeo, …). For
    generic/embedded sources the id is non-unique, so key on the exact source
    URL, which is always unique per video and never conflates distinct videos.
    The quality token keeps a 360p and a 1080p copy of the same video distinct.
    """
    keys = []
    quality = _quality_token(format_choice, format_id)
    ex = (extractor or "").strip().lower()
    if video_id and ex not in UNRELIABLE_EXTRACTORS:
        keys.append(f"id:{ex}:{video_id}:{quality}")
    if url:
        keys.append(f"url:{url.strip().lower()}:{quality}")
    return keys


def find_existing_download(video_id, extractor, url, format_choice, format_id):
    """Return an entry for an already-downloaded copy of this exact video, or None."""
    if not DEDUP_ENABLED:
        return None
    keys = _dedup_keys(video_id, extractor, url, format_choice, format_id)
    if not keys:
        return None
    with index_lock:
        removed = False
        for key in keys:
            entry = download_index.get(key)
            if not entry:
                continue
            path = entry.get("file")
            if path and os.path.exists(path):
                return dict(entry)
            if entry.get("mega_url"):
                # A remote-only item stays in history but is not a local dedup hit.
                continue
            download_index.pop(key, None)  # stale: file removed out-of-band
            removed = True
        if removed:
            _save_index_locked()
    return None


def find_existing_by_url(url):
    """Return an existing download whose original paste URL matches, or None.

    Used at fetch/info time to tell the UI whether a copy is already on the
    server before the user even clicks Download (dedup otherwise only happens
    when a job is submitted).
    """
    if not DEDUP_ENABLED or not url:
        return None
    target = url.strip().lower()
    with index_lock:
        for entry in download_index.values():
            stored = (entry.get("url") or "").strip().lower()
            path = entry.get("file")
            if stored == target and path and os.path.exists(path):
                return dict(entry)
    return None


def _make_thumb(path):
    """Generate a small preview JPG from a media file; return its path or None.

    Stored under <download dir>/thumbs so previously downloaded videos (and,
    via the lazy backfill in /api/library, older ones too) get a thumbnail that
    is shown in the Downloads sidebar.
    """
    try:
        thumbs_dir = os.path.join(DOWNLOAD_DIR, THUMBS_SUBDIR)
        os.makedirs(thumbs_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(path))[0]
        out = os.path.join(thumbs_dir, base + ".jpg")
        if os.path.exists(out) and os.path.getsize(out) > 0:
            return out
        attempts = [
            # try a frame a second into the video
            ["ffmpeg", "-y", "-v", "error", "-ss", "00:00:01", "-i", path,
             "-frames:v", "1", "-vf", "scale=320:-2", "-q:v", "6", out],
            # fall back to the very first frame for very short files
            ["ffmpeg", "-y", "-v", "error", "-i", path,
             "-frames:v", "1", "-vf", "scale=320:-2", "-q:v", "6", out],
        ]
        for cmd in attempts:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            if r.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 0:
                return out
        return None
    except Exception:
        return None


def _probe_duration(path):
    """Return the media duration in seconds via ffprobe, or None on any failure."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            return None
        val = r.stdout.strip()
        return round(float(val), 1) if val else None
    except Exception:
        return None


def register_download(video_id, extractor, title, url, format_choice, format_id, path,
                      duration=None, thumb=None, group_id=None):
    with library_mutation_lock:
        return _register_download_locked(
            video_id, extractor, title, url, format_choice, format_id, path,
            duration=duration, thumb=thumb, group_id=group_id,
        )


def _register_download_locked(video_id, extractor, title, url, format_choice, format_id,
                              path, duration=None, thumb=None, group_id=None):
    keys = _dedup_keys(video_id, extractor, url, format_choice, format_id)
    if not keys:
        return
    try:
        size_bytes = os.path.getsize(path)
    except OSError:
        size_bytes = None
    with index_lock:
        previous = next((download_index.get(key) for key in keys
                         if download_index.get(key)), None) or {}
        entry = {
            "file": path,
            "filename": os.path.basename(path),
            "title": title,
            "url": url,
            "extractor": extractor,
            "video_id": video_id,
            "created_at": previous.get("created_at") or time.time(),
            "duration": duration if duration is not None else previous.get("duration"),
            "thumb": thumb or previous.get("thumb"),
            "group_id": group_id,
            "size_bytes": size_bytes if size_bytes is not None else previous.get("size_bytes"),
            "local_deleted_at": None,
            "mega_url": previous.get("mega_url"),
            "mega_remote_path": previous.get("mega_remote_path"),
            "mega_account_id": previous.get("mega_account_id"),
            "mega_account_label": previous.get("mega_account_label"),
            "mega_uploaded_at": previous.get("mega_uploaded_at"),
        }
        for key in keys:
            download_index[key] = entry
        _save_index_locked()


def unregister_download(path):
    if not path:
        return
    with index_lock:
        stale = [k for k, v in download_index.items() if v.get("file") == path]
        for key in stale:
            download_index.pop(key, None)
        if stale:
            _save_index_locked()


def _library_identity_for_path(path):
    with index_lock:
        entry = next((value for value in download_index.values()
                      if value.get("file") == path), None)
        if not entry:
            return os.path.basename(path), ""
        return (entry.get("filename") or os.path.basename(path),
                entry.get("group_id") or "")


def _mega_upload_active(filename, group_id=""):
    helper = globals().get("mega_helper")
    return bool(helper and helper.has_active_upload(filename, group_id))


class LibraryMoveError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.message = message
        self.status = status


def _clean_library_selections(selection):
    if not isinstance(selection, list) or not selection:
        raise LibraryMoveError("Select at least one library item")
    if len(selection) > 500:
        raise LibraryMoveError("At most 500 library items can be moved at once")
    clean = []
    seen = set()
    for value in selection:
        if not isinstance(value, dict) or "group_id" not in value:
            raise LibraryMoveError("Invalid library selection")
        filename = str(value.get("filename") or "").strip()
        group_id = str(value.get("group_id") or "").strip()
        if not filename or os.path.basename(filename) != filename:
            raise LibraryMoveError("Invalid library selection")
        identity = (group_id, filename)
        if identity not in seen:
            seen.add(identity)
            clean.append({"filename": filename, "group_id": group_id})
    return clean


def _resolve_library_items_locked(selection):
    """Resolve exact group+filename identities while index_lock is held."""
    resolved = []
    claimed_paths = set()
    for wanted in selection:
        filename = wanted["filename"]
        group_id = wanted["group_id"]
        matches = [
            (key, entry) for key, entry in download_index.items()
            if (entry.get("filename") or "") == filename
            and (entry.get("group_id") or "") == group_id
            and entry.get("file")
        ]
        paths = {entry.get("file") for _key, entry in matches}
        if not paths:
            raise LibraryMoveError(f"Library item not found: {filename}", 404)
        if len(paths) != 1:
            raise LibraryMoveError(f"Library item is ambiguous: {filename}", 409)
        path = next(iter(paths))
        if path in claimed_paths:
            raise LibraryMoveError(f"Library selection is ambiguous: {filename}", 409)
        claimed_paths.add(path)
        entries = [(key, entry) for key, entry in download_index.items()
                   if entry.get("file") == path]
        representative = matches[0][1]
        local_available = os.path.isfile(path)
        if not local_available and not representative.get("mega_url"):
            raise LibraryMoveError(f"Library item no longer exists: {filename}", 404)
        resolved.append({
            "from": dict(wanted),
            "path": path,
            "entries": entries,
            "local_available": local_available,
        })
    return resolved


def _numbered_library_name(filename, target_dir, reserved_names):
    root, ext = os.path.splitext(filename)
    candidate = filename
    number = 2
    while candidate in reserved_names or os.path.exists(os.path.join(target_dir, candidate)):
        candidate = f"{root} ({number}){ext}"
        number += 1
    reserved_names.add(candidate)
    return candidate


def _rollback_library_renames(completed):
    rollback_error = None
    for source, destination in reversed(completed):
        try:
            if os.path.exists(destination):
                os.rename(destination, source)
        except OSError as exc:
            rollback_error = rollback_error or exc
    return rollback_error


def _move_library_items(selection, target_group_id):
    """Atomically reassign exact library items to a group.

    Local media is renamed into the group's directory. A retained MEGA-only
    record changes logical group/filename while keeping its historical source
    path and remote metadata untouched.
    """
    clean = _clean_library_selections(selection)
    target_group_id = str(target_group_id or "").strip()
    with library_mutation_lock:
        with groups_lock:
            if target_group_id and target_group_id not in groups:
                raise LibraryMoveError("Target group not found", 404)
            target_dir = (os.path.join(DOWNLOAD_DIR, target_group_id)
                          if target_group_id else DOWNLOAD_DIR)
            try:
                os.makedirs(target_dir, exist_ok=True)
            except OSError as exc:
                raise LibraryMoveError(f"Could not open the target group: {exc}", 500) from exc

        with index_lock:
            items = _resolve_library_items_locked(clean)
            target_names = {
                entry.get("filename") or os.path.basename(entry.get("file") or "")
                for entry in download_index.values()
                if (entry.get("group_id") or "") == target_group_id
            }

        for item in items:
            source = item["from"]
            if source["group_id"] != target_group_id and _mega_upload_active(
                    source["filename"], source["group_id"]):
                raise LibraryMoveError(
                    f"Wait for the MEGA upload and link to finish: {source['filename']}",
                    409,
                )

        planned = []
        reserved = set(target_names)
        # Names belonging to records that are already in the target remain
        # reserved. Sources in other groups do not occupy the destination.
        for item in items:
            source = item["from"]
            if source["group_id"] == target_group_id:
                planned.append({**item, "filename": source["filename"],
                                "destination": item["path"], "unchanged": True})
                reserved.add(source["filename"])
                continue
            filename = _numbered_library_name(source["filename"], target_dir, reserved)
            destination = (os.path.join(target_dir, filename)
                           if item["local_available"] else item["path"])
            planned.append({**item, "filename": filename,
                            "destination": destination, "unchanged": False})

        completed_renames = []
        try:
            with rename_lock:
                for item in planned:
                    if item["unchanged"] or not item["local_available"]:
                        continue
                    os.rename(item["path"], item["destination"])
                    completed_renames.append((item["path"], item["destination"]))
        except OSError as exc:
            rollback = _rollback_library_renames(completed_renames)
            detail = f"Could not move library files: {exc}"
            if rollback:
                detail += f" (rollback also failed: {rollback})"
            raise LibraryMoveError(detail, 500) from exc

        with index_lock:
            snapshots = {key: dict(entry) for item in planned
                         for key, entry in item["entries"]}
            try:
                for item in planned:
                    if item["unchanged"]:
                        continue
                    for _key, entry in item["entries"]:
                        if item["local_available"]:
                            entry["file"] = item["destination"]
                        entry["filename"] = item["filename"]
                        entry["group_id"] = target_group_id or None
                if any(not item["unchanged"] for item in planned):
                    _save_index_locked(strict=True)
            except Exception as exc:
                for key, snapshot in snapshots.items():
                    download_index[key].clear()
                    download_index[key].update(snapshot)
                rollback = _rollback_library_renames(completed_renames)
                detail = f"Could not save library group changes: {exc}"
                if rollback:
                    detail += f" (rollback also failed: {rollback})"
                raise LibraryMoveError(detail, 500) from exc

        for item in planned:
            if item["unchanged"]:
                continue
            for job in jobs.values():
                if job.get("file") == item["path"]:
                    if item["local_available"]:
                        job["file"] = item["destination"]
                    job["filename"] = item["filename"]
                    job["group_id"] = target_group_id or None

        return {
            "ok": True,
            "target_group_id": target_group_id,
            "moved_count": sum(not item["unchanged"] for item in planned),
            "unchanged_count": sum(item["unchanged"] for item in planned),
            "files": [{
                "from": item["from"],
                "to": {"filename": item["filename"], "group_id": target_group_id},
                "unchanged": item["unchanged"],
            } for item in planned],
        }


def _remove_local_library_file(path):
    """Remove a local media file, retaining its index rows when MEGA-backed."""
    if not path:
        return False
    with library_mutation_lock:
        try:
            size_bytes = os.path.getsize(path)
        except OSError:
            size_bytes = None
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

        with index_lock:
            matching = [key for key, entry in download_index.items()
                        if entry.get("file") == path]
            retain = any(download_index[key].get("mega_url") for key in matching)
            if retain:
                deleted_at = time.time()
                for key in matching:
                    entry = download_index[key]
                    if size_bytes is not None:
                        entry["size_bytes"] = size_bytes
                    entry["local_deleted_at"] = deleted_at
            else:
                for key in matching:
                    download_index.pop(key, None)
            if matching:
                _save_index_locked()
        return retain


def finalize_cancelled(job, job_id):
    """Mark a job stopped and remove any partial files it left behind."""
    job["status"] = "cancelled"
    job["speed"] = ""
    job["eta"] = ""
    job["proc"] = None
    remove_job_files(job_id)


class _Cancelled(Exception):
    """Raised inside run_download when the job has been stopped."""


def _attempt_download(job, job_id, cmd, timeout_msg):
    """Run one yt-dlp attempt. Returns (ok, tail); raises _Cancelled if stopped."""
    job["status"] = "downloading"
    job["progress"] = 0.0
    returncode, tail, timed_out = run_ytdlp_streaming(job, cmd, DOWNLOAD_TIMEOUT)
    if job.get("cancelled"):
        raise _Cancelled()
    if timed_out:
        job["status"] = "error"
        job["error"] = timeout_msg
        return False, tail
    if returncode != 0:
        return False, tail
    return True, tail


def _attempt_direct(job, job_id, media_url, referer, timeout_msg):
    """Last-resort stream a resolved media URL directly via curl.

    Used when yt-dlp refuses the resolved URL itself — the classic case is an
    embed/CDN whose get_file URL redirects to an opaque '.php' endpoint that
    yt-dlp skips for extension-safety reasons even though the URL streams real
    video once given the right Referer. Those streams sit behind Cloudflare,
    which blocks
    urllib/requests TLS fingerprints, so we shell out to curl with a browser
    User-Agent and Referer. Only direct files are handled (HLS stays with
    yt-dlp) and audio extraction is skipped. Returns True on success; on hard
    failure it sets the job error (like _attempt_download's timeout path) and
    returns False.
    """
    if not media_url or re.search(r"\.m3u8(?:\?|$)", media_url, re.IGNORECASE):
        return False
    if job.get("format") == "audio":
        return False

    staged = os.path.join(DOWNLOAD_DIR, f"{job_id}.mp4")
    part_path = staged + ".part"
    total = _media_total_size(media_url, referer=referer)
    started = time.monotonic()
    resume = os.path.getsize(part_path) if os.path.exists(part_path) else 0

    cmd = ["curl", "-sS", "-L", "--fail", "--max-time", str(DOWNLOAD_TIMEOUT),
           "--output", part_path, "--continue-at", str(resume),
           "-A", USER_AGENT, "-H", "Accept: */*"]
    if referer:
        cmd += ["-e", referer]
    cmd.append(media_url)

    job["status"] = "downloading"
    job["progress"] = 0.0
    proc = None
    tail = ""
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                stderr=subprocess.PIPE, text=True,
                                start_new_session=True)
        job["proc"] = proc
        prev = resume
        while True:
            if job.get("cancelled"):
                kill_proc(proc)
                raise _Cancelled()
            try:
                proc.wait(timeout=0.5)
                done = True
            except subprocess.TimeoutExpired:
                done = False
            cur = os.path.getsize(part_path) if os.path.exists(part_path) else 0
            cur = max(cur, resume)
            job["downloaded"] = _fmt_bytes(cur)
            if total:
                job["progress"] = min(round(cur / total * 100.0, 1), 100.0)
                job["total_size"] = _fmt_bytes(total)
                elapsed = max(time.monotonic() - started, 1e-6)
                speed = (cur - resume) / elapsed
                job["speed"] = f"{_fmt_bytes(int(speed))}/s"
                if speed > 0:
                    job["eta"] = f"{int((total - cur) / speed)}s"
            if done:
                break
        tail = (proc.stderr.read() or "").strip()
        rc = proc.wait()
        if rc != 0:
            lowered = (tail or "").lower()
            if "403" in lowered or "429" in lowered or "401" in lowered \
                    or "too many" in lowered or "rate limit" in lowered \
                    or "throttl" in lowered:
                job["status"] = "error"
                job["error"] = ("Rate-limited by the video host (HTTP 403). "
                                 "The host is temporarily blocking rapid requests. "
                                 "Wait a minute and press Retry.")
            else:
                job["status"] = "error"
                job["error"] = f"Direct download failed: {tail or f'curl exit {rc}'}"
            return False
    except _Cancelled:
        raise
    except Exception as e:
        job["status"] = "error"
        job["error"] = f"Direct download failed: {e}"
        return False
    finally:
        if proc is not None and proc.poll() is None:
            kill_proc(proc)
        job["proc"] = None

    if job.get("cancelled"):
        raise _Cancelled()
    if not _is_media_file(part_path):
        job["status"] = "error"
        job["error"] = "Direct download returned a non-video placeholder (token may have expired)"
        return False
    os.replace(part_path, staged)
    job["progress"] = 100.0
    job["speed"] = ""
    job["eta"] = ""
    return True


def _attempt_plain_download(job, job_id, url, referer, filename, timeout_msg):
    """Stream a resolved *file* (zip / image / clip…) straight to disk via curl.

    signed_file_host providers hand back ordinary bytes under a real name —
    there is nothing for yt-dlp to extract and its media checks would reject
    non-video payloads. This mirrors the curl loop from _attempt_direct but
    stages under the file's own extension and accepts any non-empty payload.
    Returns True on success; on failure it records the error and returns False.
    """
    ext = os.path.splitext(filename)[1].lower()
    staged = os.path.join(DOWNLOAD_DIR, f"{job_id}{ext or '.bin'}")
    part_path = staged + ".part"
    total = _media_total_size(url, referer=referer)
    started = time.monotonic()
    resume = os.path.getsize(part_path) if os.path.exists(part_path) else 0

    cmd = ["curl", "-sS", "-L", "--fail", "--max-time", str(DOWNLOAD_TIMEOUT),
           "--output", part_path, "--continue-at", str(resume),
           "-A", USER_AGENT, "-H", "Accept: */*"]
    if referer:
        cmd += ["-e", referer]
    cmd.append(url)

    job["status"] = "downloading"
    job["progress"] = 0.0
    proc = None
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                stderr=subprocess.PIPE, text=True,
                                start_new_session=True)
        job["proc"] = proc
        prev = resume
        while True:
            if job.get("cancelled"):
                kill_proc(proc)
                raise _Cancelled()
            try:
                proc.wait(timeout=0.5)
                done = True
            except subprocess.TimeoutExpired:
                done = False
            cur = os.path.getsize(part_path) if os.path.exists(part_path) else 0
            cur = max(cur, resume)
            job["downloaded"] = _fmt_bytes(cur)
            if total:
                job["progress"] = min(round(cur / total * 100.0, 1), 100.0)
                job["total_size"] = _fmt_bytes(total)
                elapsed = max(time.monotonic() - started, 1e-6)
                speed = (cur - prev) / elapsed if elapsed > 1 else None
                if speed:
                    job["speed"] = f"{_fmt_bytes(int(speed))}/s"
                    if speed > 0:
                        job["eta"] = f"{int((total - cur) / speed)}s"
                prev = cur
            if done:
                break
        tail = (proc.stderr.read() or "").strip()
        rc = proc.wait()
        if rc != 0:
            lowered = (tail or "").lower()
            if "403" in lowered or "429" in lowered or "401" in lowered:
                job["status"] = "error"
                job["error"] = ("Rate-limited by the file host (HTTP 403). "
                                 "Wait a minute and press Retry.")
            else:
                job["status"] = "error"
                job["error"] = f"File download failed: {tail or f'curl exit {rc}'}"
            return False
    except _Cancelled:
        raise
    except Exception as e:
        job["status"] = "error"
        job["error"] = f"File download failed: {e}"
        return False
    finally:
        if proc is not None and proc.poll() is None:
            kill_proc(proc)
        job["proc"] = None

    if job.get("cancelled"):
        raise _Cancelled()
    if not os.path.getsize(part_path):
        job["status"] = "error"
        job["error"] = "Downloaded file is empty (token may have expired)"
        return False
    os.replace(part_path, staged)
    job["progress"] = 100.0
    job["speed"] = ""
    job["eta"] = ""
    return True


def run_download(job_id, url, format_choice, format_id):
    job = jobs[job_id]
    out_template = os.path.join(DOWNLOAD_DIR, f"{job_id}.%(ext)s")
    timeout_msg = f"Download timed out ({DOWNLOAD_TIMEOUT // 60} min limit)"

    def fail(message):
        job["status"] = "error"
        job["error"] = message
        remove_job_files(job_id)  # don't leak partial/.part files

    acquired = False
    try:
        if job.get("cancelled"):
            finalize_cancelled(job, job_id)
            return
        # Wait for a free slot (honors the user's max-concurrent setting). The
        # job stays "queued" until a slot opens up.
        acquired = gate.acquire(job)
        if not acquired:
            finalize_cancelled(job, job_id)
            return

        # Provider-owned file hosts (signed_file_host kind) serve fixed files
        # behind a signing dance, not adaptive streams: their wrapper pages
        # mean nothing to yt-dlp, and the resolved URL is just bytes with a
        # real filename. Detect those up front and pull the file over plain
        # HTTP instead of going through the yt-dlp attempts at all.
        resolved = None
        pre = None
        if _provider_resolver_applies(url):
            job["status"] = "resolving"
            try:
                pre = resolve_provider(url)
            except Exception:
                pre = None
            if job.get("cancelled"):
                raise _Cancelled()
        if pre and pre.get("filename"):
            resolved = pre
            if not job.get("title"):
                job["title"] = resolved["filename"]
            ok = _attempt_plain_download(
                job, job_id, resolved["media_url"], resolved.get("referer"),
                resolved["filename"], timeout_msg)
            tail = [job.get("error") or "Plain download failed"]
        else:
            cmd = build_ytdlp_command(
                url, output_template=out_template,
                format_choice=format_choice, format_id=format_id,
            )
            ok, tail = _attempt_download(job, job_id, cmd, timeout_msg)

        if not ok and job.get("status") != "error" and resolved is None:
            # yt-dlp couldn't handle the page directly — try the embedded-media
            # (JW Player / iframe) fallback, then re-run against the resolved URL.
            job["status"] = "resolving"
            try:
                resolved = resolve_embedded_media(url)
            except Exception:
                resolved = None
            if job.get("cancelled"):
                raise _Cancelled()
            if not resolved:
                fail(tail[-1] if tail else "Download failed")
                return
            fallback_cmd = build_ytdlp_command(
                resolved["media_url"], output_template=out_template,
                format_choice=format_choice, format_id=format_id,
                referer=resolved.get("referer"),
            )
            ok, tail = _attempt_download(job, job_id, fallback_cmd, timeout_msg)

            if not ok and job.get("status") != "error":
                # yt-dlp refuses some resolved URLs (e.g. a CDN redirect ending
                # in '.php'), but the stream behind them is real video — pull it
                # over plain HTTP ourselves instead of giving up. The streaming
                # token is short-lived, so on a transient failure (403/429) we
                # re-resolve for a fresh, page-bound token and retry.
                direct_tries = 3
                while direct_tries > 0 and not ok:
                    ok = _attempt_direct(job, job_id, resolved["media_url"],
                                         resolved.get("referer"), timeout_msg)
                    if ok:
                        break
                    if job.get("cancelled"):
                        raise _Cancelled()
                    direct_tries -= 1
                    if direct_tries == 0:
                        break
                    err = job.get("error") or ""
                    if not any(m in err for m in ("403", "401", "429", "5")):
                        break  # non-transient — retrying won't help
                    job["status"] = "resolving"
                    try:
                        resolved = resolve_embedded_media(url)
                    except Exception:
                        resolved = None
                    if not resolved:
                        break
                    time.sleep(1)

        if not ok:
            if job.get("status") != "error":
                fail(tail[-1] if tail else "Download failed")
            else:
                remove_job_files(job_id)  # timeout set the message; still clean up
            return

        chosen = pick_output_file(job_id, format_choice)
        if not chosen:
            fail("Download completed but no file was found")
            return

        # Store the copy under a readable name, inside the chosen group folder
        # when the download was assigned to one.
        target_dir = group_dir(job.get("group_id")) or DOWNLOAD_DIR
        final_path = store_final_file(job_id, chosen, job.get("title"), target_dir=target_dir)
        job["file"] = final_path
        job["filename"] = os.path.basename(final_path)
        register_download(job.get("video_id"), job.get("extractor"),
                          job.get("title"), url, format_choice, format_id, final_path,
                          duration=_probe_duration(final_path),
                          thumb=_make_thumb(final_path),
                          group_id=job.get("group_id"))
        job["progress"] = 100.0
        job["speed"] = ""
        job["eta"] = ""
        # Set status last so a poller never sees "done" before filename exists.
        job["status"] = "done"
    except _Cancelled:
        finalize_cancelled(job, job_id)
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
        remove_job_files(job_id)
    finally:
        if acquired:
            gate.release()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/legacy")
def legacy_index():
    return render_template("legacy.html")


_fetch_info_cache = OrderedDict()
_fetch_info_inflight = {}
_fetch_info_lock = threading.Lock()


def _direct_source_info(resolved):
    """Build static card metadata for a resolved URL yt-dlp cannot inspect."""
    return {
        "title": resolved.get("title") or "",
        "thumbnail": resolved.get("thumbnail") or "",
        "duration": None,
        "uploader": "",
        "formats": [{"id": "direct", "label": "Direct source", "height": 0}],
        "id": None,
        "extractor": resolved.get("extractor") or "generic",
    }


def _extract_video_info(url):
    """Perform the expensive, cacheable portion of metadata extraction."""
    # Configured providers exist specifically for wrappers yt-dlp cannot read.
    # Resolve those first rather than waiting for a doomed 60-second yt-dlp
    # attempt. A provider miss still falls through to the generic path.
    resolved = resolve_provider(url) if _provider_resolver_applies(url) else None
    if resolved and resolved.get("filename"):
        # A signed fixed file already has its authoritative filename/metadata;
        # probing it with yt-dlp adds a process and commonly fails by design.
        return _direct_source_info(resolved)

    if resolved:
        try:
            info = run_ytdlp_json(
                resolved["media_url"], referer=resolved.get("referer"), timeout=60
            )
            if resolved.get("title"):
                info["title"] = resolved["title"]
        except (RuntimeError, ValueError):
            return _direct_source_info(resolved)
    else:
        try:
            info = run_ytdlp_json(url, timeout=60)
        except RuntimeError as primary_error:
            resolved = resolve_embedded_media(url)
            if not resolved:
                raise RuntimeError(str(primary_error))
            if resolved.get("filename"):
                return _direct_source_info(resolved)
            try:
                info = run_ytdlp_json(
                    resolved["media_url"],
                    referer=resolved.get("referer"),
                    timeout=60,
                )
                # Prefer the provider's human-readable filename over the CDN's
                # UUID path segment that direct-URL extraction usually reports.
                if resolved.get("title"):
                    info["title"] = resolved["title"]
            except (RuntimeError, ValueError):
                # The page resolved to a direct file, but yt-dlp refuses its
                # final URL. The downloader's direct HTTP fallback can still
                # consume it, so keep the card usable.
                return _direct_source_info(resolved)

    # Build quality options — keep the highest-bitrate format per resolution.
    best_by_height = {}
    for media_format in info.get("formats", []):
        height = media_format.get("height")
        if height and media_format.get("vcodec", "none") != "none":
            tbr = media_format.get("tbr") or 0
            if (
                height not in best_by_height
                or tbr > (best_by_height[height].get("tbr") or 0)
            ):
                best_by_height[height] = media_format
    formats = [
        {
            "id": media_format["format_id"],
            "label": f"{height}p",
            "height": height,
        }
        for height, media_format in sorted(best_by_height.items(), reverse=True)
    ]
    if not formats:
        # Direct-file URLs carry a single unnamed stream.
        formats = [{"id": "direct", "label": "Direct source", "height": 0}]
    return {
        "title": (resolved or {}).get("title") or info.get("title", ""),
        "thumbnail": (
            (resolved or {}).get("thumbnail") or info.get("thumbnail", "")
        ),
        "duration": info.get("duration"),
        "uploader": info.get("uploader", ""),
        "formats": formats,
        "id": info.get("id"),
        "extractor": (
            (resolved or {}).get("extractor")
            or info.get("extractor_key")
            or info.get("extractor")
            or ""
        ),
    }


def _with_existing_download(url, extracted):
    """Attach live library state to otherwise cacheable extractor metadata."""
    result = copy.deepcopy(extracted)
    existing = find_existing_by_url(url) or {}
    result["already_on_server"] = bool(existing)
    result["existing_file"] = existing.get("filename", "")
    return result


def fetch_video_info(url):
    """Extract normalized video info with TTL caching and request coalescing.

    Concurrent batches requesting the same URL share one yt-dlp/provider run.
    Successful static metadata is cached briefly; local-library fields are
    recomputed on every call so a newly downloaded/deleted file is never stale.
    Failures are shared only with current waiters and are not cached.
    """
    key = (url or "").strip()
    now = time.monotonic()
    owner = False
    extracted = None

    with _fetch_info_lock:
        cached = _fetch_info_cache.get(key)
        if cached:
            cached_at, cached_value = cached
            if now - cached_at <= FETCH_INFO_CACHE_TTL:
                _fetch_info_cache.move_to_end(key)
                extracted = cached_value
            else:
                _fetch_info_cache.pop(key, None)

        if extracted is None:
            future = _fetch_info_inflight.get(key)
            if future is None:
                future = Future()
                _fetch_info_inflight[key] = future
                owner = True

    if extracted is not None:
        return _with_existing_download(key, extracted)
    if not owner:
        return _with_existing_download(key, future.result())

    try:
        extracted = _extract_video_info(key)
    except BaseException as exc:
        with _fetch_info_lock:
            _fetch_info_inflight.pop(key, None)
            future.set_exception(exc)
        raise

    with _fetch_info_lock:
        if FETCH_INFO_CACHE_TTL > 0:
            _fetch_info_cache[key] = (time.monotonic(), extracted)
            _fetch_info_cache.move_to_end(key)
            while len(_fetch_info_cache) > FETCH_INFO_CACHE_SIZE:
                _fetch_info_cache.popitem(last=False)
        _fetch_info_inflight.pop(key, None)
        future.set_result(extracted)
    return _with_existing_download(key, extracted)


@app.route("/api/info", methods=["POST"])
def get_info():
    data = request.json
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400
    try:
        return jsonify(fetch_video_info(url))
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching video info"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/playlist", methods=["POST"])
def get_playlist_info():
    data = request.json
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    cmd = ["yt-dlp", "--flat-playlist", "-J", url]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return jsonify({"error": result.stderr.strip().split("\n")[-1]}), 400

        info = json.loads(result.stdout)
        entries = info.get("entries", [])
        urls = [entry.get("url") for entry in entries if entry.get("url")]
        return jsonify({"urls": urls})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching playlist info"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ---- Server-side fetch batches --------------------------------------------
# Info extraction for a whole batch runs on the server so a page reload never
# interrupts a bulk fetch: the client just attaches to a batch id and polls.
# Batches live in memory (same lifecycle as download jobs); after a reload the
# client re-attaches via /api/library / /api/fetch/list and keeps polling.

fetch_batches = {}
fetch_lock = threading.Lock()
FETCH_BATCH_TTL = 3600       # finished batches kept for late polls / reloads
MAX_FETCH_BATCHES = 64


def _expand_playlist_urls(url):
    """Return the individual video URLs for a playlist URL, else [url]."""
    provider_urls = _expand_via_providers(url)
    if provider_urls is not None:
        return provider_urls
    if "list=" not in url:
        return [url]
    try:
        result = subprocess.run(["yt-dlp", "--flat-playlist", "-J", url],
                                capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return [url]
        info = json.loads(result.stdout)
        urls = [e.get("url") for e in info.get("entries", []) if e.get("url")]
        return urls or [url]
    except Exception:
        return [url]


def _expand_fetch_urls(urls):
    """Expand independent playlist/provider roots concurrently, in input order."""
    roots = [(url or "").strip() for url in urls]
    roots = [url for url in roots if url]
    futures = {}
    for index, url in enumerate(roots):
        # Ordinary video URLs take the zero-cost path. Only roots that can do
        # network expansion consume a worker.
        if "list=" in url or _matching_expander(url) is not None:
            futures[index] = _fetch_expand_executor.submit(_expand_playlist_urls, url)

    expanded = []
    for index, url in enumerate(roots):
        future = futures.get(index)
        if future is None:
            expanded.append(url)
            continue
        try:
            children = future.result()
        except Exception:
            children = [url]
        expanded.extend(children or [url])
    return expanded


def _prune_fetch_batches():
    now = time.time()
    for bid, b in list(fetch_batches.items()):
        if b["finished"] and now - b.get("created_at", 0) > FETCH_BATCH_TTL:
            fetch_batches.pop(bid, None)
    if len(fetch_batches) > MAX_FETCH_BATCHES:
        for bid in list(fetch_batches)[:len(fetch_batches) - MAX_FETCH_BATCHES]:
            if fetch_batches[bid]["finished"]:
                fetch_batches.pop(bid, None)


def _fetch_url_item(url):
    return {
        "url": url, "status": "loading", "error": None,
        "title": "", "thumbnail": "", "duration": None,
        "uploader": "", "formats": [], "id": None, "extractor": "",
        "already_on_server": False, "existing_file": "",
    }


def _fetch_batch_snapshot(batch):
    """Copy persistence fields while fetch_lock protects the live batch."""
    return {
        "id": batch["id"],
        "kind": batch.get("kind") or "video",
        "urls": copy.deepcopy(batch["urls"]),
        "finished": bool(batch.get("finished")),
        "created_at": batch.get("created_at") or time.time(),
    }


def _persist_fetch_batch(batch):
    if batch.get("kind") == "scrape":
        return  # scrape batches are ephemeral (fast) and never restored
    try:
        with _db_lock:
            conn = _db_connect()
            try:
                with conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO fetch_batches VALUES (?,?,?,?,?)",
                        (batch["id"], json.dumps(batch["urls"]),
                         batch.get("created_at") or time.time(),
                         1 if batch.get("finished") else 0,
                         batch.get("kind") or "video"))
            finally:
                conn.close()
    except Exception:
        pass


def _restore_fetch_batches():
    """Recreate in-progress fetch batches from SQLite after a restart.

    Because batches are worker threads, a server restart can't resume them
    mid-flight — but instead of silently dropping the user's fetch, we rebuild
    each unfinished batch and re-run the extraction so the job keeps going.
    """
    try:
        with _db_lock:
            conn = _db_connect()
            try:
                rows = conn.execute(
                    "SELECT id, urls, created_at, finished, kind FROM fetch_batches"
                ).fetchall()
            finally:
                conn.close()
    except Exception:
        rows = []

    restored = []
    with fetch_lock:
        _prune_fetch_batches()
        for rid, urls_json, created_at, finished, _kind in rows:
            if finished or rid in fetch_batches:
                continue
            try:
                raw = json.loads(urls_json)
                urls = [u.get("url") if isinstance(u, dict) else u for u in raw]
                urls = [u for u in urls if u]
            except Exception:
                continue
            items = [_fetch_url_item(u) for u in urls]
            batch = {
                "id": rid,
                "urls": items,
                "finished": False,
                "paused": False,
                "stopped": False,
                "created_at": created_at or time.time(),
                "kind": "video",
                "_pending": len(items),
                "_futures": [],
            }
            fetch_batches[rid] = batch
            restored.append(batch)
        # drop stale finished rows from the DB
        try:
            with _db_lock:
                conn = _db_connect()
                try:
                    with conn:
                        conn.execute(
                            "DELETE FROM fetch_batches WHERE finished=1 AND created_at < ?",
                            (time.time() - FETCH_BATCH_TTL,))
                finally:
                    conn.close()
        except Exception:
            pass

    for batch in restored:
        with fetch_lock:
            for target in list(batch["urls"]):
                batch["_futures"].append(
                    _fetch_executor.submit(_process_fetch_url, batch["id"], target)
                )


def _start_fetch_batch(urls, kind="video"):
    if kind == "scrape":
        # Scrape mode: crawl each given page as-is (no playlist expansion).
        expanded = [u for u in ((u or "").strip() for u in urls) if u]
    else:
        expanded = _expand_fetch_urls(urls)

    with fetch_lock:
        _prune_fetch_batches()
        batch_id = uuid.uuid4().hex[:10]
        items = [_fetch_url_item(u) for u in expanded]
        batch = {
            "id": batch_id,
            "kind": kind,
            "urls": items,
            "finished": not expanded,
            "paused": False,
            "stopped": False,
            "created_at": time.time(),
            "_pending": len(items),
            "_futures": [],
        }
        fetch_batches[batch_id] = batch
        targets = list(batch["urls"])
        snapshot = _fetch_batch_snapshot(batch)
    # SQLite must never hold fetch_lock: polls and other workers should not wait
    # behind JSON serialization or disk I/O.
    _persist_fetch_batch(snapshot)
    with fetch_lock:
        live = fetch_batches.get(batch_id)
        if live:
            for target in targets:
                live["_futures"].append(
                    _fetch_executor.submit(_process_fetch_url, batch_id, target)
                )
    return batch_id


def _scrape_page(url, max_links=50, max_probe=24, iframe_depth=3):
    """Crawl a page/iframe graph and collect direct video links (mp4/m3u8).

    Used by the 'scrape' fetch mode: turns a page URL into a list of streamable
    media URLs the user can then download individually. Dead placeholders are
    dropped via the probe; uncertain (e.g. token-guarded) ones are kept.
    """
    out = []
    seen = set()
    visited = set()
    todo = [url]
    while todo and len(seen) < max_links + 10:
        u = todo.pop(0)
        if u in visited:
            continue
        visited.add(u)
        html = fetch_html(u)
        if not html:
            continue
        for c in find_media_candidates(html, u):
            if re.search(r"\.(jpg|jpeg|png|webp|gif)(\?|#|$)", c, re.I):
                continue
            norm = c.split("?")[0]
            if norm in seen:
                continue
            seen.add(norm)
            out.append(c)
        for ifr in find_iframe_urls(html, u)[:iframe_depth]:
            if ifr not in visited and len(todo) < iframe_depth:
                todo.append(ifr)

    kept = []
    probed = 0
    verdicts = _ordered_media_verdicts(out[:max_probe], referer=url)
    try:
        for c in out:
            if len(kept) >= max_links:
                break
            if probed < max_probe:
                probed += 1
                _candidate, verdict = next(verdicts)
                if verdict == 0:
                    continue  # definite placeholder
            kept.append(c)
    finally:
        verdicts.close()
    return kept


def _begin_fetch_finalization_locked(batch):
    """Claim the one final persistence write when no work remains."""
    if (int(batch.get("_pending", 0)) > 0 or batch.get("finished")
            or batch.get("_finalizing")):
        return None
    batch["_finalizing"] = True
    snapshot = _fetch_batch_snapshot(batch)
    snapshot["finished"] = True
    return snapshot


def _finish_fetch_finalization(batch_id, batch, snapshot):
    if snapshot is None:
        return
    try:
        _persist_fetch_batch(snapshot)
    finally:
        # A poll cannot observe finished=True until the recovery checkpoint is
        # complete. Waiting gate workers are then released so they can exit.
        with fetch_lock:
            live = fetch_batches.get(batch_id)
            if live is batch:
                live["finished"] = True
                live["paused"] = False
                live.pop("_finalizing", None)
                live.pop("_futures", None)
        fetch_gate.wake_all()


def _complete_fetch_item(batch_id, target, *, info=None, error=None, replacements=None):
    """Atomically finish a stable batch item and persist only final state."""
    snapshot = None
    with fetch_lock:
        batch = fetch_batches.get(batch_id)
        if not batch or target.get("status") not in {"loading", "fetching"}:
            return

        if replacements is not None and replacements:
            # Only scrape roots alter list shape, so only that less-common path
            # needs an identity lookup. Normal metadata completions stay O(1).
            index = next(
                (i for i, item in enumerate(batch["urls"]) if item is target), None
            )
            if index is None:
                return
            batch["urls"][index:index + 1] = replacements
        elif info is not None:
            target.update(info)
            target["status"] = "done"
        else:
            target["status"] = "error"
            target["error"] = error or "Could not fetch item"

        batch["_pending"] = max(0, int(batch.get("_pending", 1)) - 1)
        snapshot = _begin_fetch_finalization_locked(batch)

    _finish_fetch_finalization(batch_id, batch, snapshot)


def _process_fetch_url(batch_id, target):
    acquired = fetch_gate.acquire(batch_id)
    if not acquired:
        return
    try:
        with fetch_lock:
            batch = fetch_batches.get(batch_id)
            if (not batch or batch.get("stopped")
                    or target.get("status") != "loading"):
                return
            target["status"] = "fetching"
            url = target["url"]
            kind = batch.get("kind", "video")

        if kind == "scrape":
            # Crawl the page and expand it into the individual media links it
            # contains; each becomes a ready, downloadable card.
            found = _scrape_page(url)
            items = []
            for media_url in found:
                item = _fetch_url_item(media_url)
                try:
                    base = os.path.basename(urlparse(media_url).path) or media_url
                except Exception:
                    base = media_url
                item.update({"status": "done", "title": base, "url": media_url})
                items.append(item)
            if items:
                _complete_fetch_item(batch_id, target, replacements=items)
            else:
                _complete_fetch_item(
                    batch_id, target,
                    error="No video links found on this page",
                )
            return

        info = fetch_video_info(url)
        _complete_fetch_item(batch_id, target, info=info)
    except Exception as exc:
        _complete_fetch_item(batch_id, target, error=str(exc))
    finally:
        fetch_gate.release()


def _fetch_batch_public_locked(batch):
    return {
        "batch_id": batch["id"],
        "urls": [dict(item) for item in batch["urls"]],
        "finished": bool(batch.get("finished")),
        "paused": bool(batch.get("paused")),
        "stopped": bool(batch.get("stopped")),
        "kind": batch.get("kind", "video"),
    }


def _set_fetch_batch_paused(batch_id, paused):
    with fetch_lock:
        batch = fetch_batches.get(batch_id)
        if not batch:
            return None
        if not batch.get("finished") and not batch.get("stopped"):
            batch["paused"] = bool(paused)
        payload = _fetch_batch_public_locked(batch)
    fetch_gate.wake_all()
    return payload


def _stop_fetch_batch(batch_id):
    snapshot = None
    with fetch_lock:
        batch = fetch_batches.get(batch_id)
        if not batch:
            return None
        if not batch.get("finished"):
            batch["stopped"] = True
            batch["paused"] = False
            cancelled = 0
            for item in batch["urls"]:
                if item.get("status") not in {"loading", "fetching"}:
                    continue
                item["status"] = "cancelled"
                item["error"] = "Fetch stopped by user"
                cancelled += 1
            batch["_pending"] = max(
                0, int(batch.get("_pending", cancelled)) - cancelled
            )
            futures = list(batch.get("_futures") or [])
            snapshot = _begin_fetch_finalization_locked(batch)
        else:
            futures = []

    # Futures that have not entered a worker are removed from the executor.
    # Already-running network calls finish cooperatively, but their results are
    # discarded because their item status is now cancelled.
    for future in futures:
        future.cancel()
    fetch_gate.wake_all()
    _finish_fetch_finalization(batch_id, batch, snapshot)
    with fetch_lock:
        live = fetch_batches.get(batch_id)
        return _fetch_batch_public_locked(live) if live else None


@app.route("/api/fetch", methods=["POST"])
def start_fetch_batch_route():
    data = request.get_json(silent=True) or {}
    urls = data.get("urls") or []
    if not urls:
        return jsonify({"error": "No URLs provided"}), 400
    if "fetch_concurrent" in data:
        try:
            fetch_gate.set_limit(data.get("fetch_concurrent"))
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid fetch_concurrent"}), 400
    mode = data.get("mode") or "video"
    if mode not in ("video", "scrape"):
        mode = "video"
    batch_id = _start_fetch_batch(urls, kind=mode)
    return jsonify({"batch_id": batch_id, "fetch_concurrent": fetch_gate.limit})


@app.route("/api/fetch/<batch_id>")
def fetch_batch_status_route(batch_id):
    with fetch_lock:
        batch = fetch_batches.get(batch_id)
        if not batch:
            return jsonify({"error": "Batch not found"}), 404
        payload = _fetch_batch_public_locked(batch)
    return jsonify(payload)


@app.route("/api/fetch/<batch_id>/pause", methods=["POST"])
def pause_fetch_batch_route(batch_id):
    data = request.get_json(silent=True) or {}
    payload = _set_fetch_batch_paused(batch_id, data.get("paused", True))
    if payload is None:
        return jsonify({"error": "Batch not found"}), 404
    return jsonify(payload)


@app.route("/api/fetch/<batch_id>/stop", methods=["POST"])
def stop_fetch_batch_route(batch_id):
    payload = _stop_fetch_batch(batch_id)
    if payload is None:
        return jsonify({"error": "Batch not found"}), 404
    return jsonify(payload)


@app.route("/api/fetch/list")
def fetch_batch_list_route():
    with fetch_lock:
        _prune_fetch_batches()
        active = [bid for bid, b in fetch_batches.items() if not b["finished"]]
    return jsonify({"batches": active})


def _evict_old_jobs():
    """Bound the in-memory job map. Only settled failures/stops are dropped;
    'done' records are kept because /api/file still serves their file."""
    if len(jobs) <= MAX_JOBS:
        return
    removable = [jid for jid, j in jobs.items()
                 if j.get("status") in ("error", "cancelled")]
    for jid in removable[:len(jobs) - MAX_JOBS]:
        jobs.pop(jid, None)


def new_job(url, title, format_choice, format_id, batch_id=None, thumbnail=None, group_id=None):
    """Register a queued job and return its id."""
    _evict_old_jobs()
    job_id = uuid.uuid4().hex[:10]
    jobs[job_id] = {
        "status": "queued",
        "url": url,
        "title": title or "",
        "thumbnail": thumbnail or "",
        "group_id": group_id,
        "format": format_choice,
        "format_id": format_id,
        "progress": 0.0,
        "speed": "",
        "eta": "",
        "downloaded": "",
        "total_size": "",
        "filename": None,
        "file": None,
        "error": None,
        "batch_id": batch_id,
        "cancelled": False,
        "future": None,
        "proc": None,
        "video_id": None,
        "extractor": None,
        "deduped": False,
    }
    return job_id


def job_public(job_id, job):
    """The client-facing view of a job (no server filesystem paths)."""
    return {
        "job_id": job_id,
        "url": job.get("url"),
        "status": job.get("status"),
        "progress": round(job.get("progress") or 0.0, 1),
        "speed": job.get("speed") or "",
        "eta": job.get("eta") or "",
        "downloaded": job.get("downloaded") or "",
        "size": job.get("total_size") or "",
        "filename": job.get("filename"),
        "error": job.get("error"),
        "deduped": bool(job.get("deduped")),
    }


def cancel_job(job_id):
    """Stop a queued or running download. Returns True if there was work to stop."""
    job = jobs.get(job_id)
    if not job or job.get("status") in ("done", "error", "cancelled"):
        return False

    job["cancelled"] = True

    # Wake any worker parked waiting for a concurrency slot so it can bail out.
    gate.wake_all()

    # future.cancel() only succeeds if the worker hasn't started yet.
    started = True
    future = job.get("future")
    if future is not None and future.cancel():
        started = False

    # If it's already running, kill the live yt-dlp process group (yt-dlp +
    # ffmpeg); the worker thread then observes the flag and finalizes.
    proc = job.get("proc")
    if proc is not None and proc.poll() is None:
        kill_proc(proc)

    if not started:
        # Never left the queue — finalize here since no worker will.
        finalize_cancelled(job, job_id)

    return True


def _file_referenced_by_other_job(path, exclude_job_id):
    """True if any job other than exclude_job_id points at the same on-disk file."""
    return any(jid != exclude_job_id and j.get("file") == path
               for jid, j in jobs.items())


def delete_job(job_id):
    """Stop and forget a job, retaining linked library metadata when needed."""
    job = jobs.get(job_id)
    if not job:
        return False

    if job.get("status") not in ("done", "error", "cancelled"):
        cancel_job(job_id)

    # The finished file is stored under its title; staging/.part files under job id.
    # A deduped job shares its file with the original (and any other pointers),
    # so only remove/unregister the file when no other live job references it —
    # otherwise deleting a duplicate would destroy everyone's copy.
    final = job.get("file")
    if final and not _file_referenced_by_other_job(final, job_id):
        _remove_local_library_file(final)
    remove_job_files(job_id)

    jobs.pop(job_id, None)
    for batch_id, ids in list(batches.items()):
        if job_id in ids:
            remaining = [j for j in ids if j != job_id]
            if remaining:
                batches[batch_id] = remaining
            else:
                batches.pop(batch_id, None)
    return True


def apply_dedup(job):
    """If this video is already downloaded, finish the job instantly. Returns True on hit."""
    existing = find_existing_download(
        job.get("video_id"), job.get("extractor"), job.get("url"),
        job.get("format"), job.get("format_id"),
    )
    if not existing:
        return False
    job["file"] = existing["file"]
    job["filename"] = existing.get("filename") or os.path.basename(existing["file"])
    job["progress"] = 100.0
    job["deduped"] = True
    job["status"] = "done"
    return True


def launch_job(item, batch_id=None):
    """Register a job, short-circuit on dedup, else submit it to the pool. Returns job_id."""
    job_id = new_job(item["url"], item.get("title", ""), item["format"],
                     item.get("format_id"), batch_id=batch_id,
                     thumbnail=item.get("thumbnail"), group_id=item.get("group_id"))
    job = jobs[job_id]
    job["video_id"] = item.get("id")
    job["extractor"] = item.get("extractor")
    if not apply_dedup(job):
        job["future"] = download_pool.submit(
            run_download, job_id, item["url"], item["format"], item.get("format_id")
        )
    return job_id


@app.route("/api/download", methods=["POST"])
def start_download():
    data = request.json or {}
    url = data.get("url", "").strip()
    format_choice = data.get("format", "video")
    format_id = data.get("format_id")
    title = data.get("title", "")

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    job_id = launch_job({
        "url": url, "title": title, "format": format_choice,
        "format_id": format_id, "id": data.get("id"), "extractor": data.get("extractor"),
        "thumbnail": data.get("thumbnail", ""),
        "group_id": data.get("group_id"),
    })
    job = jobs[job_id]
    if job.get("deduped"):
        return jsonify({"job_id": job_id, "status": "done",
                        "deduped": True, "filename": job["filename"]})
    return jsonify({"job_id": job_id})


@app.route("/api/batch", methods=["POST"])
def start_batch():
    """Queue many downloads at once. The worker pool bounds concurrency."""
    data = request.json or {}
    items = data.get("items")
    if not items:
        # Convenience form: {urls: [...], format, format_id}
        urls = data.get("urls", [])
        fmt = data.get("format", "video")
        fid = data.get("format_id")
        items = [{"url": u, "format": fmt, "format_id": fid, "title": ""} for u in urls]

    normalized = []
    for item in items:
        item_url = (item.get("url") or "").strip()
        if not item_url:
            continue
        normalized.append({
            "url": item_url,
            "format": item.get("format", "video"),
            "format_id": item.get("format_id"),
            "title": item.get("title", ""),
            "id": item.get("id"),
            "extractor": item.get("extractor"),
            "thumbnail": item.get("thumbnail", ""),
            "group_id": item.get("group_id"),
        })

    if not normalized:
        return jsonify({"error": "No URLs provided"}), 400

    batch_id = uuid.uuid4().hex[:10]
    job_ids = [launch_job(item, batch_id=batch_id) for item in normalized]
    batches[batch_id] = job_ids

    # Order is preserved so the client can map jobs[k] back to its submitted item.
    return jsonify({
        "batch_id": batch_id,
        "jobs": [{"job_id": j, "url": jobs[j]["url"]} for j in job_ids],
    })


@app.route("/api/batch/<batch_id>")
def batch_status(batch_id):
    job_ids = batches.get(batch_id)
    if not job_ids:
        return jsonify({"error": "Batch not found"}), 404

    items = []
    total = done = errored = cancelled = 0
    progress_sum = 0.0
    for job_id in job_ids:
        job = jobs.get(job_id)
        if not job:
            continue
        total += 1
        status = job.get("status")
        if status == "done":
            done += 1
            progress_sum += 100.0
        elif status == "error":
            errored += 1
            progress_sum += 100.0  # settled (failed) items still fill the bar
        elif status == "cancelled":
            cancelled += 1
            progress_sum += 100.0  # settled (stopped) items still fill the bar
        else:
            progress_sum += job.get("progress") or 0.0
        items.append(job_public(job_id, job))

    completed = done + errored + cancelled
    overall = round(progress_sum / total, 1) if total else 0.0
    return jsonify({
        "batch_id": batch_id,
        "total": total,
        "done": done,
        "errored": errored,
        "cancelled": cancelled,
        "completed": completed,
        "overall_progress": overall,
        "finished": total > 0 and completed == total,
        "jobs": items,
    })


@app.route("/api/batches/active")
def active_batches():
    """Batch ids that still have unsettled jobs.

    The browser loses its JS state on refresh (cards and the batch id), but
    the downloads keep running server-side. This lets the UI re-attach the
    overall-progress bar to whatever is still in flight instead of silently
    hiding it after a reload.
    """
    active = []
    for batch_id, job_ids in batches.items():
        for jid in job_ids:
            status = (jobs.get(jid) or {}).get("status")
            if status in ("queued", "downloading", "processing", "resolving"):
                active.append(batch_id)
                break
    return jsonify({"batches": active})


@app.route("/api/status/<job_id>")
def check_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job_public(job_id, job))


@app.route("/api/config", methods=["GET", "POST"])
def config():
    if request.method == "POST":
        data = request.json or {}
        if "max_concurrent" in data:
            try:
                gate.set_limit(data.get("max_concurrent"))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid max_concurrent"}), 400
        if "fetch_concurrent" in data:
            try:
                fetch_gate.set_limit(data.get("fetch_concurrent"))
            except (TypeError, ValueError):
                return jsonify({"error": "Invalid fetch_concurrent"}), 400
    return jsonify({
        "max_concurrent": gate.limit,
        "max_pool": MAX_POOL,
        "fetch_concurrent": fetch_gate.limit,
        "fetch_max_pool": FETCH_MAX_WORKERS,
    })


@app.route("/api/cancel/<job_id>", methods=["POST"])
def cancel_download(job_id):
    if job_id not in jobs:
        return jsonify({"error": "Job not found"}), 404
    cancel_job(job_id)
    return jsonify(job_public(job_id, jobs[job_id]))


@app.route("/api/delete/<job_id>", methods=["POST", "DELETE"])
def delete_download(job_id):
    # Idempotent: deleting an unknown/already-gone job is a success.
    with library_mutation_lock:
        job = jobs.get(job_id)
        path = job.get("file") if job else None
        if path:
            filename, gid = _library_identity_for_path(path)
            if _mega_upload_active(filename, gid):
                return jsonify({"error": "Wait for the MEGA upload and link to finish"}), 409
        try:
            delete_job(job_id)
        except OSError as exc:
            return jsonify({"error": f"Could not delete the local file: {exc}"}), 500
    return jsonify({"ok": True})


@app.route("/api/file/<job_id>")
def download_file(job_id):
    job = jobs.get(job_id)
    if not job or job.get("status") != "done":
        return jsonify({"error": "File not ready"}), 404
    path = job.get("file")
    if not path or not os.path.exists(path):
        return jsonify({"error": "File no longer on server"}), 404
    return send_file(path, as_attachment=True, download_name=job["filename"])


@app.route("/api/library")
def library():
    """Session restore: persisted download history + live in-memory job states.

    Lets the UI repopulate the screen after a page refresh: completed files
    come from the SQLite-backed index (survives restarts), while queued / in-
    progress / errored jobs come from the live in-memory registry so their
    state and polling pick up where they left off.

    ``?group=all|ungrouped|<id>`` filters to a single group (or ungrouped).
    """
    gfilter = request.args.get("group", "all").strip() or "all"

    # Completed downloads, one entry per physical/historical path (the index
    # may hold several dedup keys pointing at that path). MEGA-backed entries
    # remain visible after their local media file has been removed.
    by_path = {}
    missing_duration = []
    missing_thumb = []
    size_changed = False
    with index_lock:
        for entry in download_index.values():
            gid = entry.get("group_id") or ""
            if gfilter == "ungrouped" and gid:
                continue
            if gfilter not in ("all", "ungrouped") and gid != gfilter:
                continue
            p = entry.get("file")
            if not p or p in by_path:
                continue
            local_available = os.path.isfile(p)
            if local_available:
                try:
                    size = os.path.getsize(p)
                    mtime = os.path.getmtime(p)
                except OSError:
                    local_available = False
                    if not entry.get("mega_url"):
                        continue
                    size = int(entry.get("size_bytes") or 0)
                    mtime = None
                else:
                    if entry.get("size_bytes") != size:
                        entry["size_bytes"] = size
                        size_changed = True
            else:
                if not entry.get("mega_url"):
                    continue
                size = int(entry.get("size_bytes") or 0)
                mtime = None
            thumb = entry.get("thumb")
            created_at = (entry.get("created_at") or mtime
                          or entry.get("mega_uploaded_at") or 0)
            by_path[p] = {
                "filename": entry.get("filename") or os.path.basename(p),
                "title": entry.get("title") or os.path.basename(p),
                "url": entry.get("url") or "",
                "size": size,
                "mtime": mtime,
                "created_at": created_at,
                "duration": entry.get("duration"),
                "has_thumb": bool(thumb and os.path.exists(thumb)),
                "group_id": gid,
                "group_name": group_name(gid),
                "local_available": local_available,
                "local_deleted_at": entry.get("local_deleted_at"),
                "mega_url": entry.get("mega_url"),
                "mega_remote_path": entry.get("mega_remote_path"),
                "mega_account_id": entry.get("mega_account_id"),
                "mega_account_label": entry.get("mega_account_label"),
                "mega_uploaded_at": entry.get("mega_uploaded_at"),
            }
            if local_available and not entry.get("duration"):
                missing_duration.append(p)
            if local_available and (not thumb or not os.path.exists(thumb)):
                missing_thumb.append(p)
        if size_changed:
            _save_index_locked()

    # Lazily backfill durations for older entries (a handful per request, so
    # browsing never stalls); results are persisted back to the index.
    if missing_duration:
        done = 0
        for p in missing_duration:
            if done >= 3:
                break
            done += 1
            dur = _probe_duration(p)
            if dur is None:
                continue
            with index_lock:
                changed = False
                for e in download_index.values():
                    if e.get("file") == p:
                        e["duration"] = dur
                        changed = True
                if changed:
                    _save_index_locked()
            if p in by_path:
                by_path[p]["duration"] = dur

    # Lazily generate thumbnails for older entries (a handful per request).
    if missing_thumb:
        done = 0
        for p in missing_thumb:
            if done >= 3:
                break
            done += 1
            thumb = _make_thumb(p)
            if not thumb:
                continue
            with index_lock:
                changed = False
                for e in download_index.values():
                    if e.get("file") == p:
                        e["thumb"] = thumb
                        changed = True
                if changed:
                    _save_index_locked()
            if p in by_path:
                by_path[p]["has_thumb"] = True

    files = sorted(by_path.values(),
                   key=lambda e: e["created_at"] or e["mtime"] or 0, reverse=True)

    # Live jobs (done jobs are covered by the file list above).
    ljobs = []
    for jid, j in list(jobs.items()):
        status = j.get("status")
        if status == "done":
            continue
        ljobs.append({
            "job_id": jid,
            "url": j.get("url") or "",
            "status": status,
            "progress": j.get("progress") or 0.0,
            "speed": j.get("speed") or "",
            "eta": j.get("eta") or "",
            "downloaded": j.get("downloaded") or "",
            "size": j.get("total_size") or "",
            "filename": j.get("filename"),
            "error": j.get("error"),
            "deduped": bool(j.get("deduped")),
            "title": j.get("title") or "",
            "thumbnail": j.get("thumbnail") or "",
            "cancelled": bool(j.get("cancelled")),
        })
    order = {"downloading": 0, "processing": 1, "resolving": 2,
             "queued": 3, "error": 4, "cancelled": 5}
    ljobs.sort(key=lambda j: (order.get(j["status"], 9), j["job_id"]))

    with fetch_lock:
        _prune_fetch_batches()
        now = time.time()
        # Keep recently-finished batches around for a short grace period too, so
        # a page reload that lands just after the batch completed still gets the
        # results instead of a blank screen.
        active_fetch = [bid for bid, b in fetch_batches.items()
                        if not b["finished"]
                        or (now - b.get("created_at", now) < 300)]
    return jsonify({"files": files, "jobs": ljobs, "fetch_batches": active_fetch})


@app.route("/api/library/download")
def library_download():
    """Serve a completed download by its registered filename (survives restarts)."""
    name = request.args.get("f", "").strip()
    gid = request.args.get("group_id")
    if not name or os.path.basename(name) != name:
        return jsonify({"error": "Bad filename"}), 400
    with index_lock:
        paths = [e.get("file") for e in download_index.values()
                 if (e.get("filename") or "") == name and e.get("file")
                 and (gid is None or (e.get("group_id") or "") == gid)]
    if not paths:
        return jsonify({"error": "File not found"}), 404
    path = paths[0]
    if not os.path.exists(path):
        return jsonify({"error": "File no longer on server"}), 404
    return send_file(path, as_attachment=True, download_name=name)


@app.route("/api/library/thumb")
def library_thumb():
    """Serve a stored preview image for a completed download (by filename)."""
    name = request.args.get("f", "").strip()
    gid = request.args.get("group_id")
    if not name or os.path.basename(name) != name:
        return jsonify({"error": "Bad filename"}), 400
    with index_lock:
        thumbs = [e.get("thumb") for e in download_index.values()
                  if (e.get("filename") or "") == name and e.get("thumb")
                  and (gid is None or (e.get("group_id") or "") == gid)]
    if not thumbs or not os.path.exists(thumbs[0]):
        return jsonify({"error": "No thumbnail"}), 404
    return send_file(thumbs[0], mimetype="image/jpeg")


@app.route("/api/library/delete", methods=["POST"])
def library_delete():
    """Delete local media; keep the library record when it has a MEGA link."""
    data = request.get_json(silent=True) or {}
    name = (data.get("f") or request.values.get("f") or "").strip()
    gid_supplied = "group_id" in data or "group_id" in request.values
    gid = str(data.get("group_id") if "group_id" in data
              else request.values.get("group_id") or "")
    if not name or os.path.basename(name) != name:
        return jsonify({"error": "Bad filename"}), 400
    with library_mutation_lock:
        with index_lock:
            paths = {e.get("file") for e in download_index.values()
                     if (e.get("filename") or "") == name and e.get("file")
                     and (not gid_supplied or (e.get("group_id") or "") == gid)}
            if not paths:
                return jsonify({"error": "File not found"}), 404
            if gid_supplied and len(paths) != 1:
                return jsonify({"error": "Library item is ambiguous"}), 409
            path = next(iter(paths))
        _matched_name, matched_gid = _library_identity_for_path(path)
        if _mega_upload_active(name, matched_gid):
            return jsonify({"error": "Wait for the MEGA upload and link to finish"}), 409
        try:
            retained = _remove_local_library_file(path)
        except OSError as exc:
            return jsonify({"error": f"Could not delete the local file: {exc}"}), 500
        for job in jobs.values():
            if job.get("file") == path:
                job["file"] = None
    return jsonify({"ok": True, "record_retained": retained})


@app.route("/api/library/group", methods=["PATCH"])
def move_library_group():
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or "target_group_id" not in data:
        return jsonify({"error": "Target group is required"}), 400
    try:
        result = _move_library_items(data.get("files"), data.get("target_group_id"))
    except LibraryMoveError as exc:
        return jsonify({"error": exc.message}), exc.status
    return jsonify(result)


@app.route("/api/groups")
def list_groups():
    """List groups plus an 'Ungrouped' pseudo-entry, each with a file count."""
    counts = {}
    seen = {}
    with index_lock:
        for e in download_index.values():
            p = e.get("file")
            if not p or p in seen:
                continue
            seen[p] = True
            gid = e.get("group_id") or ""
            counts[gid] = counts.get(gid, 0) + 1
    with groups_lock:
        items = [{"id": gid, "name": g.get("name", ""),
                  "count": counts.get(gid, 0), "created_at": g.get("created_at")}
                 for gid, g in sorted(groups.items(),
                                      key=lambda kv: kv[1].get("created_at", 0))]
    items.append({"id": "", "name": "Ungrouped", "count": counts.get("", 0)})
    return jsonify({"groups": items})


@app.route("/api/groups", methods=["POST"])
def create_group():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Name required"}), 400
    with groups_lock:
        base = _slugify(name)
        gid = base
        i = 2
        while gid in groups:
            gid = f"{base}-{i}"
            i += 1
        groups[gid] = {"name": name, "created_at": time.time()}
        _save_groups_locked()
        group_dir(gid)  # create the folder now
    return jsonify({"id": gid, "name": name}), 201


@app.route("/api/groups/<gid>", methods=["PATCH"])
def rename_group(gid):
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Name required"}), 400
    with groups_lock:
        if gid not in groups:
            return jsonify({"error": "Group not found"}), 404
        # Only the display name changes; the folder keeps its slug path.
        groups[gid]["name"] = name
        _save_groups_locked()
    return jsonify({"id": gid, "name": name})


@app.route("/api/groups/<gid>", methods=["DELETE"])
def delete_group(gid):
    """Delete a group after safely reassigning all its records to Ungrouped."""
    with library_mutation_lock:
        with groups_lock:
            if gid not in groups:
                return jsonify({"error": "Group not found"}), 404
        with index_lock:
            selection = []
            seen_paths = set()
            for entry in download_index.values():
                path = entry.get("file")
                if (entry.get("group_id") or "") != gid or not path or path in seen_paths:
                    continue
                seen_paths.add(path)
                selection.append({
                    "filename": entry.get("filename") or os.path.basename(path),
                    "group_id": gid,
                })
        if selection:
            try:
                _move_library_items(selection, "")
            except LibraryMoveError as exc:
                return jsonify({"error": exc.message}), exc.status
        with groups_lock:
            groups.pop(gid, None)
            _save_groups_locked()
        shutil.rmtree(os.path.join(DOWNLOAD_DIR, gid), ignore_errors=True)
    return jsonify({"ok": True, "moved_count": len(selection)})


# ---- MEGA helper plugin --------------------------------------------------
# The browser sends only a registered filename + group id.  Resolve that pair
# through ReClip's persisted library rather than accepting an arbitrary server
# path from the request.
def _resolve_mega_files(selection):
    try:
        clean = _clean_library_selections(selection)
        with library_mutation_lock, index_lock:
            items = _resolve_library_items_locked(clean)
            resolved = []
            for item in items:
                name = item["from"]["filename"]
                gid = item["from"]["group_id"]
                if not item["local_available"]:
                    raise MegaHelperError(f'File no longer exists: {name}', 404)
                resolved.append({
                    "path": item["path"],
                    "group_id": gid,
                    "group_name": group_name(gid),
                })
            return resolved
    except LibraryMoveError as exc:
        raise MegaHelperError(exc.message, exc.status) from exc


def _record_mega_link(upload):
    """Persist a completed upload's public URL on every dedup row for its file."""
    with library_mutation_lock:
        return _record_mega_link_locked(upload)


def _record_mega_link_locked(upload):
    public_url = str(upload.get("public_url") or "").strip()
    if not is_mega_public_url(public_url):
        raise ValueError("MEGA did not provide a valid public URL")
    name = str(upload.get("filename") or "")
    gid = str(upload.get("group_id") or "")
    source = upload.get("source_path")
    source_real = os.path.realpath(source) if source else None
    uploaded_at = upload.get("uploaded_at") or upload.get("finished_at") or time.time()
    values = {
        "mega_url": public_url,
        "mega_remote_path": upload.get("remote_path") or None,
        "mega_account_id": upload.get("account_id") or None,
        "mega_account_label": upload.get("account_label") or None,
        "mega_uploaded_at": uploaded_at,
    }
    if upload.get("size") is not None:
        values["size_bytes"] = int(upload["size"])
    matched = False
    changed = False
    with index_lock:
        for entry in download_index.values():
            path = entry.get("file")
            same_source = bool(
                source_real and path and os.path.realpath(path) == source_real
            )
            same_identity = (
                (entry.get("filename") or "") == name
                and (entry.get("group_id") or "") == gid
            )
            if not same_source and not same_identity:
                continue
            for field, value in values.items():
                if entry.get(field) != value:
                    entry[field] = value
                    changed = True
            matched = True
        if not matched:
            raise ValueError(f"Library item no longer exists: {name}")
        if changed:
            _save_index_locked(strict=True)


_init_db()
load_index()
_load_groups()
_restore_fetch_batches()
mega_helper = MegaHelper(
    DOWNLOAD_DIR,
    _resolve_mega_files,
    on_link_ready=_record_mega_link,
    operation_lock=library_mutation_lock,
)
mega_helper.reconcile_links()
app.register_blueprint(mega_helper.blueprint)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8899))
    host = os.environ.get("HOST", "127.0.0.1")
    app.run(host=host, port=port)
