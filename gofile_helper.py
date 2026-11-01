"""GoFile (gofile.io) support: expand shared folders and download their files.

How GoFile gates its API, as observed in gofile.io's own web app:

* Every visitor gets a guest account: ``POST /accounts`` returns a token. API
  calls send it as ``Authorization: Bearer <token>``. The file servers want the
  same token as an ``accountToken`` cookie and only serve files from folders
  that token has listed.
* ``GET /contents/<code>`` (the folder listing) must also carry
  ``X-Website-Token``: a SHA-256 signature computed by the obfuscated
  ``/js/wt.obf.js`` from the user agent, ``navigator.language`` (also sent as
  ``X-BL``), the account token, a 4-hour time window and a secret baked into
  the script. GoFile rotates that secret. A request signed with a retired
  secret or a future time window gets the client's IP banned for an hour.

Because of that ban, this module never re-implements the signature. It
downloads GoFile's current script, runs it inside an isolated V8 (mini-racer:
no filesystem, network or DOM) and refetches it every ``script_ttl`` seconds.
The script's clock is pinned to GoFile's server clock (taken from the API's
``Date`` header, minus a safety margin). The script is also audited while it
signs: if it reads anything besides ``navigator.userAgent`` and
``navigator.language``, its inputs have changed, so we refuse to send the
request instead of risking a ban.

Files GoFile itself refuses to serve are reported, not attempted: the listing
marks them ``overloaded`` (free downloads paused on that server) or
``isFrozen`` (moved to cold storage), and gofile.io disables both preview and
download for them.
"""

import email.utils
import json
import re
import threading
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

SITE_URL = "https://gofile.io"
API_URL = "https://api.gofile.io"
SCRIPT_PATH = "/js/wt.obf.js"

# Share codes are short alphanumeric ids; folder/file UUIDs are accepted too.
_URL_RE = re.compile(
    r"^https?://(?:www\.)?gofile\.io/d/([A-Za-z0-9][A-Za-z0-9-]{1,63})/?(?:[?#].*)?$",
    re.IGNORECASE,
)
_SCRIPT_SRC_RE = re.compile(
    r"<script[^>]+src=['\"]([^'\"]*wt[^'\"]*\.js[^'\"]*)['\"]", re.IGNORECASE
)
_WT_RE = re.compile(r"[0-9a-f]{64}")
_HTTP_CODE_RE = re.compile(r"returned error:\s*(\d{3})")

# The signing window is 4 hours and GoFile accepts the current and previous
# window, but never a future one. Running the signer a little behind GoFile's
# clock keeps a window boundary from tipping a request into the next window.
CLOCK_SAFETY_SECONDS = 10
RATE_LIMIT_COOLDOWN = 5 * 60
REFUSED_COOLDOWN = 15 * 60

# Card-sized messages: the UI truncates errors at 120 characters.
MSG_OVERLOADED = (
    "GoFile paused free downloads of this file (its server is overloaded). "
    "Try again later."
)
MSG_FROZEN = (
    "GoFile moved this file to cold storage; it can no longer be streamed or "
    "downloaded."
)
MSG_NO_LINK = "GoFile did not provide a download link for this file."
MSG_PASSWORD = "This GoFile link is password-protected."
MSG_PRIVATE = "This GoFile link is private."
MSG_EXPIRED = "This GoFile link has expired."
MSG_DENIED = "GoFile denied access to this link."
MSG_NOT_FOUND = "GoFile link not found: it was deleted or never existed."
MSG_PREMIUM = "GoFile only lets Premium accounts open this link."
MSG_EMPTY = "This GoFile folder is empty."
MSG_UNREACHABLE = "Could not reach GoFile. Check the connection and retry."
MSG_SESSION = "GoFile rejected the guest session. Try again later."
MSG_SIGNER = (
    "GoFile changed how it signs requests, so ReClip stopped to avoid an IP "
    "ban. Update ReClip."
)
MSG_NO_ENGINE = "GoFile support needs the mini-racer package (pip install mini-racer)."
MSG_RATE_LIMITED = "GoFile is rate-limiting this server."
MSG_REFUSED = "GoFile refused our requests; paused to avoid an IP ban."


class GoFileError(Exception):
    """A user-facing GoFile failure, short enough to show on a card."""


def gofile_code(url):
    """Return the share code (or UUID) of a ``gofile.io/d/<code>`` URL."""
    match = _URL_RE.match((url or "").strip())
    return match.group(1) if match else None


def is_gofile_url(url):
    return gofile_code(url) is not None


def gofile_url(code):
    return f"{SITE_URL}/d/{code}"


def _children(data):
    children = (data or {}).get("children") or {}
    if isinstance(children, dict):
        return [child for child in children.values() if isinstance(child, dict)]
    if isinstance(children, list):
        return [child for child in children if isinstance(child, dict)]
    return []


def _content_key(item):
    return item.get("code") or item.get("id")


def _gate_message(data):
    """Explain a ``canAccess: false`` gate the way gofile.io does."""
    if data.get("password"):
        return MSG_PASSWORD
    if data.get("public") is False:
        return MSG_PRIVATE
    if data.get("expire"):
        return MSG_EXPIRED
    return MSG_DENIED


def _is_gofile_file_link(link):
    try:
        parsed = urlparse(link or "")
    except ValueError:
        return False
    host = (parsed.hostname or "").lower()
    return parsed.scheme == "https" and host.endswith(".gofile.io")


def _urllib_transport(method, url, headers, data=None, timeout=20):
    """Send one HTTP request; return ``(status, lowercased headers, body)``."""
    request = Request(url, data=data, method=method, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            return (response.status,
                    {k.lower(): v for k, v in response.headers.items()},
                    response.read())
    except HTTPError as exc:
        body = exc.read() if exc.fp else b""
        return exc.code, {k.lower(): v for k, v in (exc.headers or {}).items()}, body


# Installed before GoFile's script runs. It pins Date to a caller-supplied
# instant, exposes only the two navigator fields the signature uses, and
# records any other navigator/window read so the caller can refuse to sign.
_SANDBOX_PRELUDE = r"""
(function (ua, lang) {
  var g = globalThis, unexpected = [], nowMs = null, RealDate = Date;
  function PinnedDate() {
    var now = nowMs === null ? RealDate.now() : nowMs;
    if (!new.target) return new RealDate(now).toString();
    if (arguments.length === 0) return new RealDate(now);
    return new RealDate(...arguments);
  }
  PinnedDate.now = function () { return nowMs === null ? RealDate.now() : nowMs; };
  PinnedDate.parse = RealDate.parse;
  PinnedDate.UTC = RealDate.UTC;
  PinnedDate.prototype = RealDate.prototype;
  g.Date = PinnedDate;
  var allowed = {userAgent: true, language: true};
  g.navigator = new Proxy({userAgent: ua, language: lang}, {get: function (t, k) {
    if (typeof k === 'string' && !allowed[k]) unexpected.push('navigator.' + k);
    return t[k];
  }});
  var win = new Proxy(g, {get: function (t, k) {
    if (typeof k === 'string' && !(k in t)) unexpected.push('window.' + k);
    return t[k];
  }});
  g.window = win;
  g.self = win;
  g.__reclipSign = function (token, ms) {
    unexpected.length = 0;
    nowMs = ms;
    try { var wt = g.generateWT(token); } finally { nowMs = null; }
    return JSON.stringify({wt: String(wt), unexpected: unexpected.slice()});
  };
})(%s, %s);
"""


class MiniRacerSigner:
    """Runs GoFile's ``generateWT`` inside an isolated V8 context."""

    def __init__(self, script, user_agent, language):
        try:
            from py_mini_racer import MiniRacer
        except ImportError as exc:
            raise GoFileError(MSG_NO_ENGINE) from exc
        self._ctx = MiniRacer()
        try:
            self._ctx.eval(
                _SANDBOX_PRELUDE % (json.dumps(user_agent), json.dumps(language)),
                timeout_sec=5,
            )
            self._ctx.eval(script, timeout_sec=5)
            defined = self._ctx.eval("typeof globalThis.generateWT", timeout_sec=5)
        except Exception as exc:
            self.close()
            raise GoFileError(MSG_SIGNER) from exc
        if defined != "function":
            self.close()
            raise GoFileError(MSG_SIGNER)

    def sign(self, token, now_ms):
        """Return ``(wt, unexpected_reads)`` for ``token`` at ``now_ms``."""
        raw = self._ctx.eval(
            "__reclipSign(%s, %d)" % (json.dumps(token), int(now_ms)), timeout_sec=5
        )
        payload = json.loads(raw)
        return str(payload.get("wt") or ""), list(payload.get("unexpected") or [])

    def close(self):
        try:
            self._ctx.close()
        except Exception:
            pass


class GoFileClient:
    """Thread-safe GoFile guest client: list folders, resolve file downloads.

    All API traffic is serialized under one lock, paced, and cached briefly,
    so a folder of N files costs one listing call no matter how many fetch
    and download workers touch it.
    """

    def __init__(self, user_agent, language="en-US", *, transport=None,
                 signer_factory=None, clock=time.time, sleep=time.sleep,
                 script_ttl=1800, listing_ttl=90, max_files=1000, max_depth=5,
                 page_size=1000, min_interval=0.3):
        self.user_agent = user_agent
        self.language = language
        self.script_ttl = script_ttl
        self.listing_ttl = listing_ttl
        self.max_files = max_files
        self.max_depth = max_depth
        self.page_size = page_size
        self.min_interval = min_interval
        self._transport = transport or _urllib_transport
        self._signer_factory = signer_factory or MiniRacerSigner
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.RLock()
        self._token = None
        self._skew = 0.0          # GoFile server time minus local time, seconds
        self._signer = None
        self._signer_loaded_at = 0.0
        # key -> (fetched_at, token, data | error message)
        self._listings = {}
        self._parents = {}        # file code/id -> parent folder code/id
        self._cooldown_until = 0.0
        self._cooldown_reason = ""
        self._last_call = None

    # ---- public API -------------------------------------------------------

    def expand(self, url):
        """Return one ``gofile.io/d/<code>`` URL per file under ``url``.

        Subfolders are walked breadth-first up to ``max_depth``. A subfolder
        that can't be opened is returned as its own URL, so its error shows
        on a card instead of its files silently disappearing.
        """
        code = gofile_code(url)
        if not code:
            raise GoFileError("Not a GoFile link.")
        with self._lock:
            root = self._list(code)
            if root.get("type") == "file":
                return [gofile_url(_content_key(root) or code)]
            leaves = []
            seen = {code, root.get("id"), root.get("code")}
            queue = [(root, 0)]
            while queue:
                folder, depth = queue.pop(0)
                for child in _children(folder):
                    key = _content_key(child)
                    if not key:
                        continue
                    if child.get("type") == "folder":
                        if depth + 1 > self.max_depth or key in seen:
                            continue
                        seen.update({key, child.get("id"), child.get("code")})
                        try:
                            queue.append((self._list(key), depth + 1))
                        except GoFileError:
                            leaves.append(gofile_url(key))
                    else:
                        leaves.append(gofile_url(key))
                    if len(leaves) >= self.max_files:
                        return leaves
            return leaves

    def card_info(self, url):
        """Metadata for one file's fetch card; raises GoFileError if blocked."""
        data = self._file(url)
        if data.get("type") == "folder":
            count = len(_children(data))
            if not count:
                raise GoFileError(MSG_EMPTY)
            raise GoFileError(
                f"This is a GoFile folder ({count} items). Fetch it to list its files."
            )
        self._check_downloadable(data)
        return {
            "id": data.get("id"),
            "title": data.get("name") or "",
            "thumbnail": data.get("thumbnail") or "",
            "size": data.get("size"),
            "mimetype": data.get("mimetype") or "",
        }

    def resolve_download(self, url):
        """Fresh download parameters for one file; raises GoFileError if blocked."""
        with self._lock:
            data = self._file(url)
            if data.get("type") == "folder":
                raise GoFileError(
                    "This is a GoFile folder. Fetch it first, then download its files."
                )
            self._check_downloadable(data)
            size = data.get("size")
            return {
                "media_url": data["link"],
                "filename": data.get("name") or f"{data.get('id') or 'gofile'}.bin",
                "size": size if isinstance(size, int) and size > 0 else None,
                "md5": data.get("md5") or "",
                "cookie": f"accountToken={self._token}",
                "referer": SITE_URL + "/",
            }

    def invalidate(self, url):
        """Forget cached listings for ``url`` so a retry sees fresh flags."""
        key = gofile_code(url)
        with self._lock:
            self._listings.pop(key, None)
            parent = self._parents.get(key)
            if parent:
                self._listings.pop(parent, None)

    @staticmethod
    def explain_download_error(curl_stderr):
        """Map a failed file transfer (curl --fail stderr) to a clear message."""
        match = _HTTP_CODE_RE.search(curl_stderr or "")
        code = int(match.group(1)) if match else None
        if code in (401, 403):
            return (f"GoFile refused the download (HTTP {code}): the file may be "
                    "blocked or its server overloaded. Retry later.")
        if code in (404, 410):
            return f"GoFile says this file no longer exists (HTTP {code})."
        if code == 429:
            return ("GoFile is rate-limiting downloads from this server (HTTP 429). "
                    "Wait a few minutes, then retry.")
        if code and code >= 500:
            return f"GoFile's file server failed (HTTP {code}). Retry later."
        return None

    # ---- listing + cache ---------------------------------------------------

    def _file(self, url):
        key = gofile_code(url)
        if not key:
            raise GoFileError("Not a GoFile link.")
        with self._lock:
            parent = self._parents.get(key)
            if parent:
                try:
                    folder = self._list(parent)
                except GoFileError:
                    folder = None
                for child in _children(folder):
                    # A subfolder's entry is only a stub: list it directly.
                    if (key in (child.get("code"), child.get("id"))
                            and child.get("type") != "folder"):
                        return child
            return self._list(key)

    def _list(self, key):
        with self._lock:
            now = self._clock()
            cached = self._listings.get(key)
            if (cached and now - cached[0] < self.listing_ttl
                    and cached[1] == self._token):
                if isinstance(cached[2], str):
                    raise GoFileError(cached[2])
                return cached[2]
            try:
                data = self._fetch_listing(key)
            except _PermanentError as exc:
                self._listings[key] = (self._clock(), self._token, str(exc))
                raise GoFileError(str(exc)) from None
            self._listings[key] = (self._clock(), self._token, data)
            if data.get("type") == "folder":
                for child in _children(data):
                    for child_key in (child.get("code"), child.get("id")):
                        if child_key:
                            self._parents[child_key] = key
            return data

    def _fetch_listing(self, key):
        token = self._account_token()
        renewed = False
        page = 1
        merged = None
        while True:
            status, payload = self._api(
                "GET", "/contents/" + quote(key, safe=""), token=token, signed=True,
                query={"page": page, "pageSize": self.page_size,
                       "sortField": "name", "sortDirection": 1},
            )
            api_status = (payload or {}).get("status")
            if status == 429 or api_status == "error-rateLimit":
                self._start_cooldown(RATE_LIMIT_COOLDOWN, MSG_RATE_LIMITED)
                raise GoFileError(self._cooldown_message())
            if api_status in ("error-token", "error-wrongToken", "error-accountId"):
                if renewed:
                    raise GoFileError(MSG_SESSION)
                self._reset_session()
                token = self._account_token()
                renewed = True
                page, merged = 1, None
                continue
            if status == 404 or api_status == "error-notFound":
                raise _PermanentError(MSG_NOT_FOUND)
            if api_status == "error-notPremium":
                raise _PermanentError(MSG_PREMIUM)
            if status in (401, 403):
                # Gates normally arrive as 200 + canAccess:false, so a bare
                # refusal most likely means the signature (or this IP) was
                # rejected. Back off hard and refetch the script next time.
                self._drop_signer()
                self._start_cooldown(REFUSED_COOLDOWN, MSG_REFUSED)
                raise GoFileError(self._cooldown_message())
            if status >= 500:
                raise GoFileError(f"GoFile is having problems (HTTP {status}). Try again later.")
            if status != 200 or api_status != "ok":
                detail = api_status or f"HTTP {status}"
                raise GoFileError(f"GoFile could not open this link ({detail}).")

            data = payload.get("data") or {}
            if data.get("canAccess") is False:
                raise _PermanentError(_gate_message(data))
            if merged is None:
                merged = dict(data)
                merged["children"] = {}
            for child in _children(data):
                child_key = child.get("id") or child.get("code")
                if child_key:
                    merged["children"][child_key] = child
            metadata = payload.get("metadata") or {}
            if (data.get("type") == "folder" and metadata.get("hasNextPage")
                    and len(merged["children"]) < self.max_files and page < 100):
                page += 1
                continue
            return merged

    @staticmethod
    def _check_downloadable(data):
        if data.get("canAccess") is False:
            raise GoFileError(_gate_message(data))
        if data.get("isFrozen"):
            raise GoFileError(MSG_FROZEN)
        if data.get("overloaded"):
            raise GoFileError(MSG_OVERLOADED)
        if not _is_gofile_file_link(data.get("link")):
            raise GoFileError(MSG_NO_LINK)

    # ---- session, signing, transport ----------------------------------------

    def _account_token(self):
        if self._token:
            return self._token
        status, payload = self._api("POST", "/accounts")
        token = ((payload or {}).get("data") or {}).get("token")
        if status == 429 or (payload or {}).get("status") == "error-rateLimit":
            self._start_cooldown(RATE_LIMIT_COOLDOWN, MSG_RATE_LIMITED)
            raise GoFileError(self._cooldown_message())
        if status != 200 or (payload or {}).get("status") != "ok" or not token:
            raise GoFileError(MSG_SESSION)
        self._token = token
        return token

    def _reset_session(self):
        self._token = None
        self._listings.clear()

    def _sign(self, token):
        signer = self._load_signer()
        now_ms = (self._clock() + self._skew - CLOCK_SAFETY_SECONDS) * 1000
        try:
            wt, unexpected = signer.sign(token, now_ms)
        except Exception as exc:
            self._drop_signer()
            raise GoFileError(MSG_SIGNER) from exc
        if unexpected or not _WT_RE.fullmatch(wt):
            self._drop_signer()
            raise GoFileError(MSG_SIGNER)
        return wt

    def _load_signer(self):
        now = self._clock()
        if self._signer is not None and now - self._signer_loaded_at < self.script_ttl:
            return self._signer
        script = self._download_script()
        signer = self._signer_factory(script, self.user_agent, self.language)
        self._drop_signer()
        self._signer, self._signer_loaded_at = signer, now
        return signer

    def _drop_signer(self):
        signer, self._signer = self._signer, None
        if signer is not None and hasattr(signer, "close"):
            signer.close()

    def _download_script(self):
        status, _headers, body = self._raw("GET", SITE_URL + SCRIPT_PATH)
        if status == 200 and body:
            return body.decode("utf-8", "replace")
        # The script moved: find it the way a browser would, from the page.
        status, _headers, body = self._raw("GET", SITE_URL + "/")
        match = _SCRIPT_SRC_RE.search(body.decode("utf-8", "replace")) if status == 200 else None
        if match:
            script_url = urljoin(SITE_URL + "/", match.group(1))
            if urlparse(script_url).hostname == "gofile.io":
                status, _headers, body = self._raw("GET", script_url)
                if status == 200 and body:
                    return body.decode("utf-8", "replace")
        raise GoFileError(MSG_SIGNER)

    def _api(self, method, path, *, token=None, signed=False, query=None):
        headers = {"Accept": "*/*", "Origin": SITE_URL, "Referer": SITE_URL + "/"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if signed:
            # Sign before any request goes out: a signing failure must never
            # turn into an unsigned (ban-worthy) request.
            headers["X-Website-Token"] = self._sign(token)
            headers["X-BL"] = self.language
        url = API_URL + path + ("?" + urlencode(query) if query else "")
        status, response_headers, body = self._raw(
            method, url, headers=headers, data=b"" if method == "POST" else None
        )
        self._note_server_time(response_headers)
        try:
            payload = json.loads(body.decode("utf-8", "replace"))
        except ValueError:
            payload = None
        return status, payload if isinstance(payload, dict) else None

    def _raw(self, method, url, headers=None, data=None):
        self._check_cooldown()
        self._pace()
        merged = {"User-Agent": self.user_agent}
        merged.update(headers or {})
        try:
            return self._transport(method, url, merged, data, 20)
        except OSError as exc:
            raise GoFileError(MSG_UNREACHABLE) from exc

    def _pace(self):
        now = self._clock()
        if self._last_call is not None:
            wait = self.min_interval - (now - self._last_call)
            if wait > 0:
                self._sleep(wait)
                now = self._clock()
        self._last_call = now

    def _note_server_time(self, headers):
        date = (headers or {}).get("date")
        if not date:
            return
        try:
            server = email.utils.parsedate_to_datetime(date).timestamp()
        except (TypeError, ValueError, IndexError, OverflowError):
            return
        self._skew = server - self._clock()

    def _start_cooldown(self, seconds, reason):
        self._cooldown_until = max(self._cooldown_until, self._clock() + seconds)
        self._cooldown_reason = reason

    def _cooldown_message(self):
        remaining = max(0.0, self._cooldown_until - self._clock())
        minutes = max(1, int((remaining + 59) // 60))
        return f"{self._cooldown_reason} Retry in ~{minutes} min."

    def _check_cooldown(self):
        if self._clock() < self._cooldown_until:
            raise GoFileError(self._cooldown_message())


class _PermanentError(GoFileError):
    """A listing outcome worth caching (deleted, gated, premium-only)."""
