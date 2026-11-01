"""Telegram uploader for ReClip.

Uploads finished downloads to a Telegram chat (Saved Messages or a private
channel) through a regular user account via Telethon. User accounts can send
files up to 2000 MiB (4000 MiB with Premium); bigger videos are split into
parts with ffmpeg (stream copy, no re-encode) before uploading.

The ReClip web UI uses TelegramService (bottom of this file): browser sign-in,
chat discovery and a queue of album posts. The CLI shares the same state:
    python telegram_helper.py login            # one-time interactive sign-in
    python telegram_helper.py chats            # list your chats and their ids
    python telegram_helper.py use <chat>       # set the default destination
    python telegram_helper.py upload FILE...   # upload to the default chat

State lives in $RECLIP_TELEGRAM_DIR (default: <download dir>/.telegram). The
session file there is a full login to the Telegram account — keep it private.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import glob
import json
import math
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from typing import Callable

MIB = 1024 * 1024
# Telegram uploads are at most 4000 (8000 with Premium) parts of 512 KiB.
FREE_LIMIT = 2000 * MIB
PREMIUM_LIMIT = 4000 * MIB
# ffmpeg can only cut on keyframes, so parts overshoot their target size.
SPLIT_HEADROOM = 0.9
SESSION_NAME = "reclip"

Progress = Callable[[str, int, int], None]


class TelegramHelperError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


# ----------------------------------------------------------------------
# State and configuration


def state_dir() -> str:
    base = os.environ.get("RECLIP_TELEGRAM_DIR")
    if not base:
        downloads = os.environ.get("RECLIP_DOWNLOAD_DIR") or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "downloads"
        )
        base = os.path.join(downloads, ".telegram")
    os.makedirs(base, mode=0o700, exist_ok=True)
    try:
        os.chmod(base, 0o700)
    except OSError:
        pass
    return base


def _config_path() -> str:
    return os.path.join(state_dir(), "config.json")


def load_config() -> dict:
    try:
        with open(_config_path(), encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_config(cfg: dict) -> None:
    path = _config_path()
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".config-")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(cfg, fh, indent=2)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def credentials(cfg: dict, *, interactive: bool) -> tuple[int, str]:
    api_id = os.environ.get("TG_API_ID") or cfg.get("api_id")
    api_hash = os.environ.get("TG_API_HASH") or cfg.get("api_hash")
    if not (api_id and api_hash):
        if not interactive:
            raise TelegramHelperError(
                "Telegram is not set up yet. Run: python telegram_helper.py login"
            )
        print("Create an app at https://my.telegram.org → API development tools,")
        print("then paste its credentials here (they are saved in", state_dir() + ").")
        api_id = input("api_id: ").strip()
        api_hash = input("api_hash: ").strip()
        if not api_id.isdigit() or not api_hash:
            raise TelegramHelperError("api_id must be a number and api_hash is required.")
        cfg.update(api_id=int(api_id), api_hash=api_hash)
        save_config(cfg)
    return int(api_id), str(api_hash)


def parse_chat(value: str | int) -> str | int:
    """Numeric ids (e.g. -1001234567890) become ints; usernames/links stay strings."""
    if isinstance(value, int):
        return value
    value = value.strip()
    return int(value) if value.lstrip("-").isdigit() else value


# ----------------------------------------------------------------------
# ffmpeg helpers


def probe_video(path: str) -> dict | None:
    """Return {duration, w, h} for files with a video stream, else None."""
    try:
        out = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=width,height:format=duration",
                "-of", "json", path,
            ],
            capture_output=True, text=True, timeout=60, check=True,
        ).stdout
        data = json.loads(out)
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    streams = data.get("streams") or []
    if not streams:
        return None
    try:
        duration = float((data.get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    return {
        "duration": max(duration, 0.0),
        "w": int(streams[0].get("width") or 0),
        "h": int(streams[0].get("height") or 0),
    }


def make_thumbnail(path: str, info: dict, out_path: str) -> str | None:
    """Grab one frame scaled to Telegram's 320px thumbnail box."""
    at = min(5.0, info["duration"] / 3) if info.get("duration") else 0.0
    try:
        subprocess.run(
            [
                "ffmpeg", "-v", "error", "-y", "-ss", f"{at:.2f}", "-i", path,
                "-frames:v", "1",
                "-vf", "scale=320:320:force_original_aspect_ratio=decrease",
                "-q:v", "5", out_path,
            ],
            capture_output=True, timeout=60, check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out_path if os.path.isfile(out_path) and os.path.getsize(out_path) else None


def plan_parts(size: int, limit: int) -> int:
    return max(1, math.ceil(size / (limit * SPLIT_HEADROOM)))


def split_video(path: str, info: dict | None, limit: int, workdir: str) -> list[str]:
    """Split *path* into keyframe-aligned parts that each fit under *limit*."""
    if not info or info["duration"] <= 0:
        raise TelegramHelperError(
            f"{os.path.basename(path)} is over the Telegram size limit and is not "
            "a video ffmpeg can split."
        )
    size = os.path.getsize(path)
    count = plan_parts(size, limit)
    stem, ext = os.path.splitext(os.path.basename(path))
    ext = ext or ".mp4"
    pattern = os.path.join(workdir, f"part_%03d{ext}")
    try:
        subprocess.run(
            [
                "ffmpeg", "-v", "error", "-y", "-i", path,
                "-map", "0:v:0", "-map", "0:a:0?", "-c", "copy",
                "-f", "segment", "-segment_time", f"{info['duration'] / count:.3f}",
                "-reset_timestamps", "1", pattern,
            ],
            capture_output=True, text=True, check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise TelegramHelperError(f"ffmpeg could not split {path}: {exc.stderr.strip()}")
    parts = sorted(glob.glob(os.path.join(workdir, f"part_*{ext}")))
    if not parts:
        raise TelegramHelperError(f"ffmpeg produced no parts for {path}")
    oversized = [p for p in parts if os.path.getsize(p) > limit]
    if oversized:
        raise TelegramHelperError(
            f"{os.path.basename(path)}: a split part is still over the limit "
            "(very sparse keyframes). Re-encode it first."
        )
    named = []
    for i, part in enumerate(parts, 1):
        target = os.path.join(workdir, f"{stem} (part {i} of {len(parts)}){ext}")
        os.replace(part, target)
        named.append(target)
    return named


# ----------------------------------------------------------------------
# Telegram


def make_client():
    from telethon import TelegramClient

    cfg = load_config()
    api_id, api_hash = credentials(cfg, interactive=False)
    return TelegramClient(os.path.join(state_dir(), SESSION_NAME), api_id, api_hash)


async def connect_authorized():
    client = make_client()
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise TelegramHelperError(
            "Not signed in to Telegram. Run: python telegram_helper.py login"
        )
    return client


async def resolve_chat(client, chat: str | int):
    chat = parse_chat(chat)
    try:
        return await client.get_entity(chat)
    except ValueError:
        # Numeric ids are only resolvable once the dialog list has been cached.
        await client.get_dialogs()
        return await client.get_entity(chat)


def message_link(entity, message_id: int) -> str | None:
    from telethon.tl.types import Channel

    if not isinstance(entity, Channel):
        return None
    if getattr(entity, "username", None):
        return f"https://t.me/{entity.username}/{message_id}"
    return f"https://t.me/c/{entity.id}/{message_id}"


async def upload_limit(client) -> int:
    me = await client.get_me()
    return PREMIUM_LIMIT if getattr(me, "premium", False) else FREE_LIMIT


async def upload_file(client, entity, path: str, limit: int, progress: Progress | None = None) -> dict:
    """Upload one file (split if needed). Returns message ids and links."""
    from telethon.tl.types import DocumentAttributeVideo

    path = os.path.abspath(path)
    if not os.path.isfile(path):
        raise TelegramHelperError(f"Not a file: {path}")
    name = os.path.basename(path)
    info = probe_video(path)
    workdir = tempfile.mkdtemp(prefix=".reclip-tg-", dir=os.path.dirname(path))
    try:
        if os.path.getsize(path) <= limit:
            parts = [path]
        else:
            parts = split_video(path, info, limit, workdir)
        message_ids, links = [], []
        for i, part in enumerate(parts, 1):
            part_info = info if part == path else probe_video(part)
            caption = name if len(parts) == 1 else f"{name} (part {i}/{len(parts)})"
            kwargs: dict = {}
            if part_info:
                kwargs["supports_streaming"] = True
                kwargs["attributes"] = [
                    DocumentAttributeVideo(
                        duration=part_info["duration"],
                        w=part_info["w"],
                        h=part_info["h"],
                        supports_streaming=True,
                    )
                ]
                thumb = make_thumbnail(part, part_info, os.path.join(workdir, f"thumb_{i}.jpg"))
                if thumb:
                    kwargs["thumb"] = thumb
            callback = None
            if progress:
                callback = lambda sent, total, label=caption: progress(label, sent, total)
            msg = await client.send_file(
                entity, part, caption=caption, progress_callback=callback, **kwargs
            )
            message_ids.append(msg.id)
            link = message_link(entity, msg.id)
            if link:
                links.append(link)
        return {"file": path, "parts": len(parts), "message_ids": message_ids, "links": links}
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ----------------------------------------------------------------------
# Web service: browser sign-in and album posts for the ReClip UI

ALBUM_MAX = 10  # Telegram's limit for one grouped message
CAPTION_LIMIT = 1024
PREMIUM_CAPTION_LIMIT = 4096
PARSE_MODES = {"plain": None, "markdown": "md", "html": "html"}
POST_ACTIVE = {"queued", "uploading", "sending"}
POST_FINAL = {"done", "error", "cancelled"}
MAX_POST_FILES = 100
MAX_POSTS_KEPT = 200
API_HASH_RE = re.compile(r"^[0-9a-fA-F]{32}$")
PHONE_RE = re.compile(r"^\+?[0-9][0-9 ()-]{5,20}$")


def album_chunks(count: int, size: int = ALBUM_MAX) -> list[int]:
    """Split *count* media into the fewest albums, balanced (11 -> 6 + 5)."""
    if count <= 0:
        return []
    albums = math.ceil(count / size)
    base, extra = divmod(count, albums)
    return [base + (1 if i < extra else 0) for i in range(albums)]


def caption_length(text: str, parse_mode: str | None) -> int:
    """Visible caption length in UTF-16 code units, the way Telegram counts it."""
    if parse_mode == "md":
        from telethon.extensions import markdown

        text, _ = markdown.parse(text)
    elif parse_mode == "html":
        from telethon.extensions import html

        text, _ = html.parse(text)
    return len(text.encode("utf-16-le")) // 2


def _mask_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    return f"+{digits[:2]}{'•' * max(0, len(digits) - 4)}{digits[-2:]}" if digits else ""


def _describe_rpc_error(exc: Exception) -> str:
    from telethon import errors

    if isinstance(exc, errors.FloodWaitError):
        minutes = max(1, math.ceil(exc.seconds / 60))
        return f"Telegram rate limit hit. Try again in about {minutes} min."
    if isinstance(exc, errors.ChatWriteForbiddenError):
        return "You can't post in that chat. Pick a channel where you're an admin."
    if isinstance(exc, errors.RPCError):
        return f"Telegram refused the request: {getattr(exc, 'message', None) or exc}"
    return str(exc) or exc.__class__.__name__


class TelegramService:
    """Owns the Telegram client (on its own event loop) and the post queue.

    Flask threads talk to the client only through :meth:`_run`, which hands a
    coroutine to the loop thread. Posts run one at a time: a single account
    uploading in parallel mostly earns flood waits, not speed.
    """

    def __init__(
        self,
        file_resolver: Callable[[list[dict]], list[dict]],
        *,
        on_posted: Callable[[dict], None] | None = None,
        client_factory: Callable[[int, str, str], object] | None = None,
    ) -> None:
        self.file_resolver = file_resolver
        self.on_posted = on_posted
        self.client_factory = client_factory
        self.dir = state_dir()
        self.posts_path = os.path.join(self.dir, "posts.json")
        self._lock = threading.RLock()
        self._posts: dict[str, dict] = {}
        self._rates: dict[str, tuple[float, int, float]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue | None = None
        self._tasks: dict[str, asyncio.Task] = {}
        self._client = None
        self._me = None
        self._login: dict = {}
        self._chats: tuple[float, list[dict]] = (0.0, [])
        self._load_posts()
        self.blueprint = self._make_blueprint()
        if any(p.get("status") == "queued" for p in self._posts.values()):
            self._ensure_loop()  # resume posts that were waiting when ReClip stopped

    # -- availability / loop -------------------------------------------

    @staticmethod
    def available() -> tuple[bool, str | None]:
        try:
            import telethon  # noqa: F401
        except ImportError:
            return False, "Telethon is not installed. Run: pip install -r requirements.txt"
        return True, None

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is not None:
                return self._loop
            loop = asyncio.new_event_loop()
            ready = threading.Event()

            def run() -> None:
                asyncio.set_event_loop(loop)
                self._queue = asyncio.Queue()
                loop.create_task(self._worker())
                ready.set()
                loop.run_forever()

            threading.Thread(target=run, name="telegram-loop", daemon=True).start()
            ready.wait(10)
            self._loop = loop
            queued = [p["id"] for p in self._sorted_posts(oldest_first=True) if p["status"] == "queued"]
        for post_id in queued:
            loop.call_soon_threadsafe(self._queue.put_nowait, post_id)
        return loop

    def _run(self, coro, timeout: float = 90):
        future = asyncio.run_coroutine_threadsafe(coro, self._ensure_loop())
        try:
            return future.result(timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise TelegramHelperError("Telegram did not respond in time. Try again.", 504)

    # -- client / auth ---------------------------------------------------

    async def _get_client(self):
        if self._client is None:
            api_id, api_hash = credentials(load_config(), interactive=False)
            session = os.path.join(self.dir, SESSION_NAME)
            if self.client_factory:
                self._client = self.client_factory(api_id, api_hash, session)
            else:
                from telethon import TelegramClient

                # Few, quick retries: the UI is waiting on this connection.
                self._client = TelegramClient(
                    session, api_id, api_hash, connection_retries=2, retry_delay=1, timeout=15
                )
        if not self._client.is_connected():
            await self._client.connect()
        return self._client

    async def _authorized_client(self):
        client = await self._get_client()
        if not await client.is_user_authorized():
            raise TelegramHelperError("Sign in to Telegram first.", 401)
        if self._me is None:
            self._me = await client.get_me()
        return client

    async def _drop_client(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                pass
        self._client, self._me, self._login = None, None, {}
        self._chats = (0.0, [])

    def _premium(self) -> bool:
        return bool(getattr(self._me, "premium", False))

    async def _status(self) -> dict:
        cfg = load_config()
        configured = bool(cfg.get("api_id") and cfg.get("api_hash")) or bool(
            os.environ.get("TG_API_ID") and os.environ.get("TG_API_HASH")
        )
        out = {
            "available": True,
            "configured": configured,
            "authorized": False,
            "me": None,
            "login_step": self._login.get("step"),
            "login_phone": _mask_phone(self._login.get("phone", "")),
            "default_chat": str(cfg["chat"]) if cfg.get("chat") is not None else None,
            "default_chat_title": cfg.get("chat_title"),
            "limits": {"file": FREE_LIMIT, "caption": CAPTION_LIMIT, "album": ALBUM_MAX},
        }
        if not configured:
            return out
        client = await self._get_client()
        if not await client.is_user_authorized():
            return out
        if self._me is None:
            self._me = await client.get_me()
        me = self._me
        out["authorized"] = True
        out["me"] = {
            "name": " ".join(filter(None, [getattr(me, "first_name", ""), getattr(me, "last_name", "")])) or "Telegram user",
            "username": getattr(me, "username", None),
            "premium": self._premium(),
        }
        if self._premium():
            out["limits"].update(file=PREMIUM_LIMIT, caption=PREMIUM_CAPTION_LIMIT)
        return out

    def status(self) -> dict:
        ok, error = self.available()
        if not ok:
            return {"available": False, "error": error}
        try:
            return self._run(self._status())
        except (OSError, ConnectionError) as exc:
            return {"available": True, "configured": True, "authorized": False, "error": f"Can't reach Telegram: {exc}"}

    def set_credentials(self, api_id, api_hash) -> None:
        api_id = str(api_id or "").strip()
        api_hash = str(api_hash or "").strip()
        if not api_id.isdigit() or len(api_id) > 12:
            raise TelegramHelperError("api_id must be the number shown on my.telegram.org.")
        if not API_HASH_RE.match(api_hash):
            raise TelegramHelperError("api_hash must be the 32-character code shown on my.telegram.org.")
        if self.has_active_posts():
            raise TelegramHelperError("Wait for the running Telegram posts to finish or cancel them first.", 409)
        self._run(self._drop_client())
        cfg = load_config()
        cfg.update(api_id=int(api_id), api_hash=api_hash)
        save_config(cfg)

    async def _send_code(self, phone: str) -> dict:
        from telethon import errors

        client = await self._get_client()
        try:
            sent = await client.send_code_request(phone)
        except errors.PhoneNumberInvalidError:
            raise TelegramHelperError("That phone number isn't valid. Include the country code, e.g. +90…")
        except errors.PhoneNumberBannedError:
            raise TelegramHelperError("Telegram has banned this phone number.", 403)
        except errors.ApiIdInvalidError:
            raise TelegramHelperError("Telegram rejected the api_id/api_hash. Re-enter them.", 401)
        except errors.RPCError as exc:
            raise TelegramHelperError(_describe_rpc_error(exc), 429 if isinstance(exc, errors.FloodWaitError) else 400)
        via_app = type(getattr(sent, "type", None)).__name__ == "SentCodeTypeApp"
        self._login = {"step": "code", "phone": phone, "hash": sent.phone_code_hash}
        return {"sent_to": "Telegram app" if via_app else "SMS"}

    def send_code(self, phone) -> dict:
        phone = str(phone or "").strip()
        if not PHONE_RE.match(phone):
            raise TelegramHelperError("Enter your phone number with country code, e.g. +90 555 123 45 67.")
        return self._run(self._send_code(re.sub(r"[ ()-]", "", phone)))

    async def _verify_code(self, code: str) -> dict:
        from telethon import errors

        if self._login.get("step") != "code":
            raise TelegramHelperError("Request a login code first.", 409)
        client = await self._get_client()
        try:
            await client.sign_in(phone=self._login["phone"], code=code, phone_code_hash=self._login["hash"])
        except errors.SessionPasswordNeededError:
            self._login["step"] = "password"
            return {"needs_password": True}
        except errors.PhoneCodeInvalidError:
            raise TelegramHelperError("That code is wrong. Check the latest message from Telegram.")
        except errors.PhoneCodeExpiredError:
            self._login = {}
            raise TelegramHelperError("That code expired. Request a new one.", 410)
        except errors.PhoneNumberUnoccupiedError:
            self._login = {}
            raise TelegramHelperError("There's no Telegram account for this number. Sign up in the app first.")
        except errors.RPCError as exc:
            raise TelegramHelperError(_describe_rpc_error(exc))
        self._login, self._me = {}, None
        return {"needs_password": False}

    def verify_code(self, code) -> dict:
        code = re.sub(r"\D", "", str(code or ""))
        if not 4 <= len(code) <= 8:
            raise TelegramHelperError("Enter the login code Telegram sent you.")
        return self._run(self._verify_code(code))

    async def _verify_password(self, password: str) -> None:
        from telethon import errors

        if self._login.get("step") != "password":
            raise TelegramHelperError("Enter the login code first.", 409)
        client = await self._get_client()
        try:
            await client.sign_in(password=password)
        except errors.PasswordHashInvalidError:
            raise TelegramHelperError("Wrong two-step verification password.")
        except errors.RPCError as exc:
            raise TelegramHelperError(_describe_rpc_error(exc))
        self._login, self._me = {}, None

    def verify_password(self, password) -> None:
        if not password:
            raise TelegramHelperError("Enter your two-step verification password.")
        self._run(self._verify_password(str(password)))

    async def _cancel_login(self) -> None:
        self._login = {}

    def cancel_login(self) -> None:
        self._run(self._cancel_login())

    async def _logout(self) -> None:
        client = await self._get_client()
        if await client.is_user_authorized():
            await client.log_out()  # also deletes the session file
        await self._drop_client()

    def logout(self) -> None:
        if self.has_active_posts():
            raise TelegramHelperError("Wait for the running Telegram posts to finish or cancel them first.", 409)
        self._run(self._logout())

    # -- chats -----------------------------------------------------------

    async def _list_chats(self, refresh: bool) -> list[dict]:
        cached_at, cached = self._chats
        if cached and not refresh and time.monotonic() - cached_at < 120:
            return cached
        from telethon.tl.types import Channel, Chat

        client = await self._authorized_client()
        chats = [{"id": "me", "title": "Saved Messages", "kind": "saved", "username": None}]
        async for dialog in client.iter_dialogs(limit=500):
            entity = dialog.entity
            if isinstance(entity, Channel):
                if entity.broadcast:
                    rights = getattr(entity, "admin_rights", None)
                    if not (entity.creator or (rights and rights.post_messages)):
                        continue
                    kind = "channel"
                else:
                    kind = "group"
            elif isinstance(entity, Chat):
                if getattr(entity, "deactivated", False) or getattr(entity, "left", False):
                    continue
                kind = "group"
            else:
                continue
            chats.append({
                "id": str(dialog.id),
                "title": dialog.name or "Untitled",
                "kind": kind,
                "username": getattr(entity, "username", None),
            })
        self._chats = (time.monotonic(), chats)
        return chats

    def list_chats(self, refresh: bool = False) -> list[dict]:
        return self._run(self._list_chats(refresh))

    async def _chat_title(self, chat: str) -> str:
        client = await self._authorized_client()
        try:
            entity = await resolve_chat(client, chat)
        except (ValueError, TypeError):
            raise TelegramHelperError("Telegram couldn't find that chat. Refresh the chat list and pick it again.", 404)
        if chat == "me":
            return "Saved Messages"
        return getattr(entity, "title", None) or getattr(entity, "first_name", None) or str(chat)

    def set_default_chat(self, chat) -> dict:
        chat = str(chat or "").strip()
        if not chat:
            raise TelegramHelperError("Choose a chat.")
        title = self._run(self._chat_title(chat))
        cfg = load_config()
        cfg.update(chat=parse_chat(chat), chat_title=title)
        save_config(cfg)
        return {"chat": chat, "title": title}

    # -- posts: persistence ----------------------------------------------

    def _load_posts(self) -> None:
        try:
            with open(self.posts_path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            data = []
        now = time.time()
        for post in data if isinstance(data, list) else []:
            if not isinstance(post, dict) or not post.get("id"):
                continue
            if post.get("status") in ("uploading", "sending"):
                post.update(status="error", error="Interrupted because ReClip restarted. Retry to send it again.",
                            finished_at=now, current=None)
            self._posts[post["id"]] = post

    def _save_posts_locked(self) -> None:
        os.makedirs(self.dir, mode=0o700, exist_ok=True)
        posts = self._sorted_posts()[:MAX_POSTS_KEPT]
        fd, tmp = tempfile.mkstemp(dir=self.dir, prefix=".posts-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(posts, fh)
            os.replace(tmp, self.posts_path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _sorted_posts(self, oldest_first: bool = False) -> list[dict]:
        return sorted(self._posts.values(), key=lambda p: p.get("created_at") or 0, reverse=not oldest_first)

    def _update(self, post_id: str, persist: bool = True, **fields) -> dict | None:
        with self._lock:
            post = self._posts.get(post_id)
            if post is None:
                return None
            post.update(fields)
            if persist:
                self._save_posts_locked()
            return dict(post)

    def _update_item(self, post_id: str, index: int, **fields) -> None:
        with self._lock:
            post = self._posts.get(post_id)
            if post and 0 <= index < len(post["items"]):
                post["items"][index].update(fields)
                self._save_posts_locked()

    @staticmethod
    def _public_post(post: dict) -> dict:
        out = {k: v for k, v in post.items() if not k.startswith("_")}
        out["items"] = [{k: v for k, v in item.items() if k != "path"} for item in post.get("items", [])]
        return out

    def list_posts(self) -> list[dict]:
        with self._lock:
            return [self._public_post(p) for p in self._sorted_posts()]

    def has_active_posts(self) -> bool:
        with self._lock:
            return any(p.get("status") in POST_ACTIVE for p in self._posts.values())

    def has_active_upload(self, filename: str, group_id: str = "") -> bool:
        with self._lock:
            return any(
                post.get("status") in POST_ACTIVE
                and any((item.get("filename") or "") == filename and (item.get("group_id") or "") == (group_id or "")
                        for item in post.get("items", []))
                for post in self._posts.values()
            )

    # -- posts: API --------------------------------------------------------

    def create_post(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            raise TelegramHelperError("Invalid request.")
        files = payload.get("files")
        if not isinstance(files, list) or not files:
            raise TelegramHelperError("Choose at least one video.")
        if len(files) > MAX_POST_FILES:
            raise TelegramHelperError(f"Send at most {MAX_POST_FILES} videos at once.")
        caption = str(payload.get("caption") or "").strip()
        mode = str(payload.get("parse_mode") or "plain")
        if mode not in PARSE_MODES:
            raise TelegramHelperError("Unknown text format.")
        chat = str(payload.get("chat") or load_config().get("chat") or "").strip()
        if not chat:
            raise TelegramHelperError("Choose where to send the videos.")

        resolved = self.file_resolver(files)
        # Signs in, resolves the chat and checks the caption against the account's limit.
        title = self._run(self._chat_title(chat))
        limit = PREMIUM_CAPTION_LIMIT if self._premium() else CAPTION_LIMIT
        try:
            length = caption_length(caption, PARSE_MODES[mode])
        except Exception:
            raise TelegramHelperError("The text has broken formatting. Check the Markdown/HTML.")
        if length > limit:
            raise TelegramHelperError(f"The text is {length} characters; Telegram allows {limit} under media.")

        now = time.time()
        items = [{
            "filename": f["filename"],
            "group_id": f.get("group_id") or "",
            "path": f["path"],
            "size": int(f.get("size") or 0),
            "uploaded": 0,
            "parts": 1,
            "status": "queued",
        } for f in resolved]
        post = {
            "id": uuid.uuid4().hex[:12],
            "status": "queued",
            "chat": chat,
            "chat_title": title,
            "caption": caption,
            "parse_mode": mode,
            "silent": bool(payload.get("silent")),
            "items": items,
            "albums": album_chunks(len(items)),
            "total_bytes": sum(i["size"] for i in items),
            "uploaded_bytes": 0,
            "speed": None,
            "eta": None,
            "current": None,
            "message_ids": [],
            "links": [],
            "error": None,
            "created_at": now,
            "started_at": None,
            "finished_at": None,
        }
        with self._lock:
            self._posts[post["id"]] = post
            self._save_posts_locked()
        loop = self._ensure_loop()
        loop.call_soon_threadsafe(self._queue.put_nowait, post["id"])
        return self._public_post(post)

    def cancel_post(self, post_id: str) -> dict:
        with self._lock:
            post = self._posts.get(post_id)
            if post is None:
                raise TelegramHelperError("Post not found.", 404)
            if post["status"] == "queued":
                post.update(status="cancelled", finished_at=time.time())
                self._save_posts_locked()
                return self._public_post(post)
            if post["status"] not in POST_ACTIVE:
                raise TelegramHelperError("This post already finished.", 409)
            task = self._tasks.get(post_id)
        if task is not None and self._loop is not None:
            self._loop.call_soon_threadsafe(task.cancel)
        with self._lock:
            return self._public_post(self._posts[post_id])

    def retry_post(self, post_id: str) -> dict:
        with self._lock:
            post = self._posts.get(post_id)
            if post is None:
                raise TelegramHelperError("Post not found.", 404)
            if post["status"] not in ("error", "cancelled"):
                raise TelegramHelperError("Only failed or cancelled posts can be retried.", 409)
            payload = {
                "files": [{"filename": i["filename"], "group_id": i["group_id"]} for i in post["items"]],
                "chat": post["chat"],
                "caption": post["caption"],
                "parse_mode": post["parse_mode"],
                "silent": post.get("silent", False),
            }
        return self.create_post(payload)

    def clear_finished(self) -> int:
        with self._lock:
            finished = [pid for pid, p in self._posts.items() if p.get("status") in POST_FINAL]
            for pid in finished:
                self._posts.pop(pid, None)
            self._save_posts_locked()
            return len(finished)

    # -- posts: worker -----------------------------------------------------

    async def _worker(self) -> None:
        while True:
            post_id = await self._queue.get()
            with self._lock:
                post = self._posts.get(post_id)
                if not post or post.get("status") != "queued":
                    continue
            task = asyncio.ensure_future(self._process(post_id))
            self._tasks[post_id] = task
            await asyncio.wait([task])  # never raises, even when the task is cancelled
            self._tasks.pop(post_id, None)
            self._rates.pop(post_id, None)

    def _on_progress(self, post_id: str, index: int, base: int, sent: int) -> None:
        with self._lock:
            post = self._posts.get(post_id)
            if not post:
                return
            post["items"][index]["uploaded"] = base + sent
            uploaded = sum(i["uploaded"] for i in post["items"])
            post["uploaded_bytes"] = uploaded
            now = time.monotonic()
            last_t, last_b, speed = self._rates.get(post_id, (now, uploaded, 0.0))
            if now - last_t >= 0.5:
                instant = (uploaded - last_b) / (now - last_t)
                speed = instant if not speed else speed * 0.7 + instant * 0.3
                self._rates[post_id] = (now, uploaded, speed)
                post["speed"] = speed
                remaining = max(0, post["total_bytes"] - uploaded)
                post["eta"] = remaining / speed if speed > 0 else None
            elif post_id not in self._rates:
                self._rates[post_id] = (now, uploaded, 0.0)

    async def _upload_media(self, client, post_id: str, index: int, path: str, info: dict | None,
                            base: int, workdir: str):
        from telethon.tl.types import (
            DocumentAttributeFilename,
            DocumentAttributeVideo,
            InputMediaUploadedDocument,
        )

        name = os.path.basename(path)
        self._update(post_id, current=name)
        uploaded = await client.upload_file(
            path, progress_callback=lambda sent, _total: self._on_progress(post_id, index, base, sent)
        )
        attributes = [DocumentAttributeFilename(name)]
        thumb = None
        if info:
            attributes.insert(0, DocumentAttributeVideo(
                duration=info["duration"], w=info["w"], h=info["h"], supports_streaming=True,
            ))
            thumb_path = await asyncio.to_thread(
                make_thumbnail, path, info, os.path.join(workdir, f"thumb-{uuid.uuid4().hex[:8]}.jpg")
            )
            if thumb_path:
                thumb = await client.upload_file(thumb_path)
        return InputMediaUploadedDocument(
            file=uploaded,
            mime_type=mimetypes.guess_type(path)[0] or "video/mp4",
            attributes=attributes,
            thumb=thumb,
        )

    async def _process(self, post_id: str) -> None:
        workdir = tempfile.mkdtemp(prefix="work-", dir=self.dir)
        try:
            client = await self._authorized_client()
            with self._lock:
                post = json.loads(json.dumps(self._posts[post_id]))
            entity = await resolve_chat(client, post["chat"])
            limit = PREMIUM_LIMIT if self._premium() else FREE_LIMIT
            self._update(post_id, status="uploading", started_at=time.time(), error=None)

            # Each library file becomes one media unit, or several if it must be split.
            units: list[tuple[int, str, dict | None]] = []
            for index, item in enumerate(post["items"]):
                path = item["path"]
                if not os.path.isfile(path):
                    raise TelegramHelperError(f"File no longer exists: {item['filename']}")
                info = await asyncio.to_thread(probe_video, path)
                if os.path.getsize(path) <= limit:
                    parts = [path]
                else:
                    self._update_item(post_id, index, status="splitting")
                    part_dir = tempfile.mkdtemp(prefix="split-", dir=workdir)
                    parts = await asyncio.to_thread(split_video, path, info, limit, part_dir)
                    info = None  # each part gets probed on its own
                    self._update_item(post_id, index, parts=len(parts), size=sum(os.path.getsize(p) for p in parts))
                units.extend((index, part, info) for part in parts)
            with self._lock:
                live = self._posts[post_id]
                live["total_bytes"] = sum(i["size"] for i in live["items"])
                live["albums"] = album_chunks(len(units))
                self._save_posts_locked()

            send_kwargs = {
                "parse_mode": PARSE_MODES[post["parse_mode"]],
                "silent": post.get("silent") or None,
                "supports_streaming": True,
            }
            item_base: dict[int, int] = {}
            message_ids: list[int] = []
            links: list[str] = []
            position = 0
            for album_index, count in enumerate(album_chunks(len(units))):
                media = []
                for index, part, info in units[position:position + count]:
                    self._update_item(post_id, index, status="uploading")
                    if info is None:
                        info = await asyncio.to_thread(probe_video, part)
                    base = item_base.get(index, 0)
                    media.append(await self._upload_media(client, post_id, index, part, info, base, workdir))
                    item_base[index] = base + os.path.getsize(part)
                    self._on_progress(post_id, index, 0, item_base[index])
                position += count
                self._update(post_id, status="sending", current=None)
                caption = post["caption"] if album_index == 0 else ""
                if len(media) == 1:
                    sent = [await client.send_file(entity, media[0], caption=caption, **send_kwargs)]
                else:
                    sent = await client.send_file(entity, media, caption=[caption], **send_kwargs)
                ids = [m.id for m in sent]
                message_ids.extend(ids)
                link = message_link(entity, ids[0]) if ids else None
                if link:
                    links.append(link)
                with self._lock:
                    live = self._posts[post_id]
                    for index, _part, _info in units[position - count:position]:
                        live["items"][index]["status"] = "sent"
                    live.update(message_ids=list(message_ids), links=list(links),
                                status="uploading" if position < len(units) else "sending")
                    self._save_posts_locked()

            final = {"status": "done", "finished_at": time.time(), "current": None, "speed": None, "eta": None}
            with self._lock:
                final["uploaded_bytes"] = self._posts[post_id]["total_bytes"]
                snapshot = {**self._posts[post_id], **final}
            # Record the library badge before flipping to "done", so a UI refresh
            # triggered by the status change already sees it.
            if self.on_posted:
                try:
                    await asyncio.to_thread(self.on_posted, snapshot)
                except Exception:
                    pass  # the post itself succeeded; the library badge is best-effort
            self._update(post_id, **final)
        except asyncio.CancelledError:
            self._update(post_id, status="cancelled", finished_at=time.time(), current=None, speed=None, eta=None,
                         error="Cancelled. Albums that were already sent stay in the chat."
                         if self._posts.get(post_id, {}).get("message_ids") else None)
        except TelegramHelperError as exc:
            self._update(post_id, status="error", error=exc.message, finished_at=time.time(), current=None,
                         speed=None, eta=None)
        except Exception as exc:  # RPC errors, network drops, ffmpeg failures
            self._update(post_id, status="error", error=_describe_rpc_error(exc), finished_at=time.time(),
                         current=None, speed=None, eta=None)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    # -- Flask routes --------------------------------------------------------

    def _make_blueprint(self):
        from flask import Blueprint, jsonify, request

        bp = Blueprint("telegram_helper", __name__, url_prefix="/api/telegram")

        def body() -> dict:
            data = request.get_json(silent=True)
            return data if isinstance(data, dict) else {}

        def guarded(fn):
            def wrapper(*args, **kwargs):
                try:
                    return fn(*args, **kwargs)
                except TelegramHelperError as exc:
                    return jsonify({"error": exc.message}), exc.status
                except (OSError, ConnectionError) as exc:
                    return jsonify({"error": f"Can't reach Telegram: {exc}"}), 502
            wrapper.__name__ = fn.__name__
            return wrapper

        @bp.get("/status")
        @guarded
        def status():
            return jsonify(self.status())

        @bp.post("/credentials")
        @guarded
        def save_credentials():
            data = body()
            self.set_credentials(data.get("api_id"), data.get("api_hash"))
            return jsonify(self.status())

        @bp.post("/login/code")
        @guarded
        def login_code():
            return jsonify(self.send_code(body().get("phone")))

        @bp.post("/login/verify")
        @guarded
        def login_verify():
            return jsonify(self.verify_code(body().get("code")))

        @bp.post("/login/password")
        @guarded
        def login_password():
            self.verify_password(body().get("password"))
            return jsonify({"ok": True})

        @bp.post("/login/cancel")
        @guarded
        def login_cancel():
            self.cancel_login()
            return jsonify({"ok": True})

        @bp.post("/logout")
        @guarded
        def logout():
            self.logout()
            return jsonify({"ok": True})

        @bp.get("/chats")
        @guarded
        def chats():
            return jsonify({"chats": self.list_chats(request.args.get("refresh") == "1")})

        @bp.post("/default-chat")
        @guarded
        def default_chat():
            return jsonify(self.set_default_chat(body().get("chat")))

        @bp.get("/posts")
        @guarded
        def posts():
            return jsonify({"posts": self.list_posts()})

        @bp.post("/posts")
        @guarded
        def create():
            return jsonify({"post": self.create_post(body())}), 201

        @bp.post("/posts/<post_id>/cancel")
        @guarded
        def cancel(post_id):
            return jsonify({"post": self.cancel_post(post_id)})

        @bp.post("/posts/<post_id>/retry")
        @guarded
        def retry(post_id):
            return jsonify({"post": self.retry_post(post_id)}), 201

        @bp.delete("/posts/finished")
        @guarded
        def clear():
            return jsonify({"removed": self.clear_finished()})

        return bp


# ----------------------------------------------------------------------
# CLI


class _ConsoleProgress:
    def __init__(self) -> None:
        self.label = None
        self.started = 0.0

    def __call__(self, label: str, sent: int, total: int) -> None:
        if label != self.label:
            self.label, self.started = label, time.monotonic()
            print(f"→ {label}")
        elapsed = max(time.monotonic() - self.started, 1e-6)
        pct = sent / total if total else 1.0
        print(
            f"\r  {pct:6.1%}  {sent / MIB:,.0f}/{total / MIB:,.0f} MiB  "
            f"{sent / MIB / elapsed:5.1f} MiB/s",
            end="\n" if sent >= total else "",
            flush=True,
        )


async def _login() -> None:
    from telethon import TelegramClient

    cfg = load_config()
    api_id, api_hash = credentials(cfg, interactive=True)
    client = TelegramClient(os.path.join(state_dir(), SESSION_NAME), api_id, api_hash)
    await client.start()  # prompts for phone, login code and 2FA password
    try:
        me = await client.get_me()
        handle = f" (@{me.username})" if me.username else ""
        print(f"Signed in as {me.first_name or ''}{handle}. Premium: {bool(getattr(me, 'premium', False))}")
        print("Next: python telegram_helper.py chats   then   python telegram_helper.py use <id>")
    finally:
        await client.disconnect()


async def _chats() -> None:
    client = await connect_authorized()
    try:
        print(f"{'id':>16}  name")
        print(f"{'me':>16}  Saved Messages")
        async for dialog in client.iter_dialogs():
            kind = "channel" if dialog.is_channel and not dialog.is_group else (
                "group" if dialog.is_group else "user"
            )
            print(f"{dialog.id:>16}  {dialog.name}  [{kind}]")
    finally:
        await client.disconnect()


async def _use(chat: str) -> None:
    client = await connect_authorized()
    try:
        entity = await resolve_chat(client, chat)
    finally:
        await client.disconnect()
    cfg = load_config()
    cfg["chat"] = parse_chat(chat)
    save_config(cfg)
    title = getattr(entity, "title", None) or getattr(entity, "first_name", None) or chat
    print(f"Uploads will go to: {title}")


async def _upload(files: list[str], chat: str | None) -> int:
    chat = chat or load_config().get("chat")
    if not chat:
        raise TelegramHelperError(
            "No destination chat. Run: python telegram_helper.py use <chat>  (or pass --chat)"
        )
    client = await connect_authorized()
    failures = 0
    try:
        entity = await resolve_chat(client, chat)
        limit = await upload_limit(client)
        progress = _ConsoleProgress()
        for path in files:
            try:
                result = await upload_file(client, entity, path, limit, progress)
            except TelegramHelperError as exc:
                failures += 1
                print(f"✗ {exc}", file=sys.stderr)
                continue
            parts = f" in {result['parts']} parts" if result["parts"] > 1 else ""
            print(f"✓ {os.path.basename(path)}{parts}")
            for link in result["links"]:
                print(f"  {link}")
    finally:
        await client.disconnect()
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upload ReClip downloads to Telegram.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("login", help="sign in with your Telegram account (one time)")
    sub.add_parser("chats", help="list chats and their ids")
    use = sub.add_parser("use", help="set the default destination chat")
    use.add_argument("chat", help="'me', a numeric id from `chats`, or a @username")
    upload = sub.add_parser("upload", help="upload files")
    upload.add_argument("files", nargs="+")
    upload.add_argument("--chat", help="override the default destination")
    args = parser.parse_args(argv)

    try:
        if args.command == "login":
            asyncio.run(_login())
        elif args.command == "chats":
            asyncio.run(_chats())
        elif args.command == "use":
            asyncio.run(_use(args.chat))
        elif args.command == "upload":
            return asyncio.run(_upload(args.files, args.chat))
    except TelegramHelperError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
