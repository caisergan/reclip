"""Multi-account MEGA storage and upload plugin for ReClip.

The plugin deliberately exposes only account administration, local -> MEGA
uploads, and public-link creation for those uploads. It does not expose
arbitrary local paths or remote download/delete operations. rclone's MEGA
backend is used for authentication, storage quota queries, transfers and links.
"""

from __future__ import annotations

import configparser
import json
import os
import re
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from typing import Callable, Iterable
from urllib.parse import urlsplit

from flask import Blueprint, jsonify, request


ACCOUNT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{2,47}$")
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
TRANSFER_STATES = {"queued", "uploading"}
ACTIVE_STATES = {*TRANSFER_STATES, "linking"}
MEGA_PUBLIC_HOSTS = {"mega.nz", "mega.co.nz"}


def is_mega_public_url(value: str) -> bool:
    """Return whether *value* uses an official MEGA public-link host."""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    host = (parsed.hostname or "").lower()
    return parsed.scheme == "https" and any(
        host == allowed or host.endswith(f".{allowed}")
        for allowed in MEGA_PUBLIC_HOSTS
    )
FINAL_STATES = {"done", "error", "cancelled"}


class MegaHelperError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


class MegaHelper:
    """Owns MEGA accounts, quota cache and bounded upload workers."""

    def __init__(
        self,
        download_dir: str,
        file_resolver: Callable[[list[dict]], list[dict]],
        *,
        state_dir: str | None = None,
        rclone_bin: str | None = None,
        max_workers: int | None = None,
        on_link_ready: Callable[[dict], None] | None = None,
        operation_lock: object | None = None,
    ) -> None:
        self.download_dir = os.path.realpath(download_dir)
        self.file_resolver = file_resolver
        self.state_dir = os.path.realpath(
            state_dir
            or os.environ.get("RECLIP_MEGA_DIR")
            or os.path.join(self.download_dir, ".mega")
        )
        self.rclone_bin = rclone_bin or os.environ.get("RECLIP_RCLONE", "rclone")
        self.on_link_ready = on_link_ready
        self.operation_lock = operation_lock or nullcontext()
        self.max_workers = max(
            1,
            min(int(max_workers or os.environ.get("RECLIP_MEGA_CONCURRENT", "2")), 8),
        )
        self.quota_ttl = max(10, int(os.environ.get("RECLIP_MEGA_QUOTA_TTL", "60")))
        self.safety_bytes = max(
            0, int(os.environ.get("RECLIP_MEGA_SAFETY_MB", "16")) * 1024 * 1024
        )
        self.max_jobs = max(20, int(os.environ.get("RECLIP_MEGA_MAX_JOBS", "300")))

        os.makedirs(self.state_dir, mode=0o700, exist_ok=True)
        try:
            os.chmod(self.state_dir, 0o700)
        except OSError:
            pass

        self.accounts_path = os.path.join(self.state_dir, "accounts.json")
        self.jobs_path = os.path.join(self.state_dir, "uploads.json")
        self.rclone_config = os.path.join(self.state_dir, "rclone.conf")
        self._lock = threading.RLock()
        self._config_lock = threading.Lock()
        self._accounts: dict[str, dict] = {}
        self._jobs: dict[str, dict] = {}
        self._quota_cache: dict[str, dict] = {}
        self._availability: dict | None = None
        self._pool = ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="mega-upload"
        )

        self._load_accounts()
        self._load_jobs()
        self.blueprint = Blueprint("mega_helper", __name__, url_prefix="/api/mega")
        self._register_routes()

    # ------------------------------------------------------------------
    # Persistence and rclone configuration

    @staticmethod
    def _atomic_json(path: str, value: object) -> None:
        directory = os.path.dirname(path)
        fd, temp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(value, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_path, 0o600)
            os.replace(temp_path, path)
        finally:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass

    def _load_accounts(self) -> None:
        try:
            with open(self.accounts_path, encoding="utf-8") as handle:
                raw = json.load(handle)
            items = raw.get("accounts", []) if isinstance(raw, dict) else []
            self._accounts = {
                item["id"]: item
                for item in items
                if isinstance(item, dict) and ACCOUNT_ID_RE.match(str(item.get("id", "")))
            }
        except (OSError, ValueError, TypeError):
            self._accounts = {}

    def _save_accounts_locked(self) -> None:
        ordered = sorted(self._accounts.values(), key=lambda item: item.get("created_at", 0))
        self._atomic_json(self.accounts_path, {"version": 1, "accounts": ordered})

    def _load_jobs(self) -> None:
        try:
            with open(self.jobs_path, encoding="utf-8") as handle:
                raw = json.load(handle)
            items = raw.get("uploads", []) if isinstance(raw, dict) else []
        except (OSError, ValueError, TypeError):
            items = []
        now = time.time()
        for item in items:
            if not isinstance(item, dict) or not item.get("id"):
                continue
            if item.get("status") in ACTIVE_STATES:
                if item.get("public_url"):
                    item["status"] = "done"
                    item["error"] = None
                else:
                    item["status"] = "error"
                    item["error"] = "Upload was interrupted when ReClip restarted"
                item["finished_at"] = now
            self._jobs[item["id"]] = item
        self._prune_jobs_locked()
        if items:
            self._save_jobs_locked()

    def _save_jobs_locked(self) -> None:
        persisted = []
        for job in sorted(self._jobs.values(), key=lambda item: item.get("created_at", 0)):
            persisted.append({k: v for k, v in job.items() if not k.startswith("_")})
        self._atomic_json(self.jobs_path, {"version": 1, "uploads": persisted})

    def _prune_jobs_locked(self) -> None:
        if len(self._jobs) <= self.max_jobs:
            return
        finished = sorted(
            (j for j in self._jobs.values() if j.get("status") in FINAL_STATES),
            key=lambda item: item.get("finished_at") or item.get("created_at", 0),
        )
        for job in finished[: max(0, len(self._jobs) - self.max_jobs)]:
            self._jobs.pop(job["id"], None)

    def _rclone_command(
        self,
        args: Iterable[str],
        *,
        timeout: int = 45,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                [self.rclone_bin, *args],
                input=input_text,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except FileNotFoundError as exc:
            raise MegaHelperError("rclone is not installed", 503) from exc
        except subprocess.TimeoutExpired as exc:
            raise MegaHelperError("MEGA request timed out", 504) from exc

    @staticmethod
    def _last_error(proc: subprocess.CompletedProcess, fallback: str) -> str:
        text = (proc.stderr or proc.stdout or "").strip()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return (lines[-1] if lines else fallback)[:500]

    def availability(self, force: bool = False) -> dict:
        with self._lock:
            if self._availability is not None and not force:
                return dict(self._availability)
        try:
            proc = self._rclone_command(["version"], timeout=10)
            version = (proc.stdout.splitlines() or [""])[0].strip()
            providers = self._rclone_command(["help", "backends"], timeout=10)
            has_mega = providers.returncode == 0 and bool(
                re.search(r"(?m)^\s*mega\s+Mega\s*$", providers.stdout)
            )
            result = {
                "available": bool(proc.returncode == 0 and has_mega),
                "version": version,
                "error": None if has_mega else "This rclone build has no MEGA backend",
            }
        except MegaHelperError as exc:
            result = {"available": False, "version": "", "error": exc.message}
        with self._lock:
            self._availability = result
        return dict(result)

    def _ensure_available(self) -> None:
        status = self.availability()
        if not status["available"]:
            raise MegaHelperError(status.get("error") or "MEGA helper is unavailable", 503)

    def _read_rclone_config_locked(self) -> configparser.RawConfigParser:
        config = configparser.RawConfigParser(interpolation=None)
        config.optionxform = str
        if os.path.exists(self.rclone_config):
            config.read(self.rclone_config, encoding="utf-8")
        return config

    def _write_rclone_config_locked(self, config: configparser.RawConfigParser) -> None:
        fd, temp_path = tempfile.mkstemp(prefix=".rclone-", dir=self.state_dir, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                config.write(handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_path, 0o600)
            os.replace(temp_path, self.rclone_config)
        finally:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass

    def _obscure_password(self, password: str) -> str:
        proc = self._rclone_command(["obscure", "-"], input_text=password + "\n", timeout=10)
        if proc.returncode != 0 or not proc.stdout.strip():
            raise MegaHelperError(self._last_error(proc, "Could not secure the password"), 500)
        return proc.stdout.strip().splitlines()[-1]

    @staticmethod
    def _slug(value: str) -> str:
        value = value.lower().strip()
        value = re.sub(r"[^a-z0-9_-]+", "-", value).strip("-_")
        return value[:32] or "account"

    def _set_remote(self, remote: str, email: str, obscured_password: str) -> None:
        with self._config_lock:
            config = self._read_rclone_config_locked()
            if config.has_section(remote):
                config.remove_section(remote)
            config.add_section(remote)
            config.set(remote, "type", "mega")
            config.set(remote, "user", email)
            config.set(remote, "pass", obscured_password)
            # MEGA encrypts payloads itself, but HTTPS also protects metadata
            # and avoids ISP throttling of plain HTTP transfer connections.
            config.set(remote, "use_https", "true")
            self._write_rclone_config_locked(config)

    def _delete_remote(self, remote: str) -> None:
        with self._config_lock:
            config = self._read_rclone_config_locked()
            if config.remove_section(remote):
                self._write_rclone_config_locked(config)

    # ------------------------------------------------------------------
    # Accounts and quota

    def _query_quota(self, account: dict) -> dict:
        self._ensure_available()
        proc = self._rclone_command(
            [
                "about",
                f"{account['remote']}:",
                "--json",
                "--config",
                self.rclone_config,
                "--contimeout",
                "20s",
                "--timeout",
                "30s",
            ],
            timeout=45,
        )
        if proc.returncode != 0:
            raise MegaHelperError(self._last_error(proc, "Could not read MEGA quota"), 502)
        try:
            value = json.loads(proc.stdout)
            total = int(value["total"])
            used = int(value["used"])
            free = int(value.get("free", max(0, total - used)))
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            raise MegaHelperError("MEGA returned an invalid quota response", 502) from exc
        return {
            "total": max(0, total),
            "used": max(0, used),
            "free": max(0, free),
            "checked_at": time.time(),
            "error": None,
        }

    def refresh_quota(self, account_id: str, force: bool = False) -> dict:
        with self._lock:
            account = self._accounts.get(account_id)
            if not account:
                raise MegaHelperError("MEGA account not found", 404)
            cached = self._quota_cache.get(account_id)
            if cached and not force and time.time() - cached.get("checked_at", 0) < self.quota_ttl:
                return dict(cached)
            account = dict(account)
        try:
            quota = self._query_quota(account)
        except MegaHelperError as exc:
            quota = {
                "total": None,
                "used": None,
                "free": None,
                "checked_at": time.time(),
                "error": exc.message,
            }
        with self._lock:
            self._quota_cache[account_id] = quota
        return dict(quota)

    def refresh_all_quotas(self, force: bool = False) -> None:
        with self._lock:
            ids = list(self._accounts)
        if not ids:
            return
        with ThreadPoolExecutor(max_workers=min(4, len(ids))) as pool:
            list(pool.map(lambda account_id: self.refresh_quota(account_id, force), ids))

    def _reserved_locked(self) -> dict[str, int]:
        result: dict[str, int] = {}
        for job in self._jobs.values():
            if job.get("status") in TRANSFER_STATES:
                account_id = job.get("account_id")
                result[account_id] = result.get(account_id, 0) + int(job.get("size") or 0)
        return result

    def public_accounts(self, refresh: bool = False) -> list[dict]:
        self.refresh_all_quotas(force=refresh)
        with self._lock:
            reserved = self._reserved_locked()
            result = []
            for account in sorted(
                self._accounts.values(), key=lambda item: item.get("created_at", 0)
            ):
                quota = dict(self._quota_cache.get(account["id"], {}))
                held = reserved.get(account["id"], 0)
                free = quota.get("free")
                result.append(
                    {
                        "id": account["id"],
                        "label": account["label"],
                        "email": account["email"],
                        "enabled": bool(account.get("enabled", True)),
                        "created_at": account.get("created_at"),
                        "quota": quota or None,
                        "reserved": held,
                        "available_for_upload": (
                            max(0, int(free) - held - self.safety_bytes)
                            if isinstance(free, int)
                            else None
                        ),
                    }
                )
            return result

    def add_account(self, label: str, email: str, password: str) -> dict:
        self._ensure_available()
        label = label.strip()
        email = email.strip()
        if not label or len(label) > 60:
            raise MegaHelperError("Account label is required (maximum 60 characters)")
        if not EMAIL_RE.match(email) or len(email) > 254:
            raise MegaHelperError("Enter a valid MEGA email address")
        if not password or len(password) > 1024:
            raise MegaHelperError("MEGA password is required")
        with self._lock:
            if any(a["email"].lower() == email.lower() for a in self._accounts.values()):
                raise MegaHelperError("This MEGA account is already configured", 409)
            account_id = f"{self._slug(label)}-{uuid.uuid4().hex[:8]}"
            remote = f"reclip_mega_{account_id}"
        obscured = self._obscure_password(password)
        self._set_remote(remote, email, obscured)
        candidate = {
            "id": account_id,
            "remote": remote,
            "label": label,
            "email": email,
            "enabled": True,
            "created_at": time.time(),
        }
        try:
            quota = self._query_quota(candidate)
        except MegaHelperError:
            self._delete_remote(remote)
            raise
        with self._lock:
            self._accounts[account_id] = candidate
            self._quota_cache[account_id] = quota
            self._save_accounts_locked()
        return self._public_account(account_id)

    def _public_account(self, account_id: str) -> dict:
        items = self.public_accounts(refresh=False)
        for item in items:
            if item["id"] == account_id:
                return item
        raise MegaHelperError("MEGA account not found", 404)

    def update_account(self, account_id: str, data: dict) -> dict:
        with self._lock:
            current = self._accounts.get(account_id)
            if not current:
                raise MegaHelperError("MEGA account not found", 404)
            updated = dict(current)
        if "label" in data:
            label = str(data.get("label") or "").strip()
            if not label or len(label) > 60:
                raise MegaHelperError("Account label is required (maximum 60 characters)")
            updated["label"] = label
        if "enabled" in data:
            updated["enabled"] = bool(data.get("enabled"))

        password = data.get("password")
        if password is not None:
            password = str(password)
            if not password or len(password) > 1024:
                raise MegaHelperError("MEGA password is required")
            obscured = self._obscure_password(password)
            with self._config_lock:
                config = self._read_rclone_config_locked()
                old_pass = config.get(updated["remote"], "pass", fallback="")
            self._set_remote(updated["remote"], updated["email"], obscured)
            try:
                quota = self._query_quota(updated)
            except MegaHelperError:
                self._set_remote(updated["remote"], updated["email"], old_pass)
                raise
            with self._lock:
                self._quota_cache[account_id] = quota

        with self._lock:
            self._accounts[account_id] = updated
            self._save_accounts_locked()
        return self._public_account(account_id)

    def delete_account(self, account_id: str) -> None:
        with self._lock:
            account = self._accounts.get(account_id)
            if not account:
                return
            if any(
                j.get("account_id") == account_id and j.get("status") in ACTIVE_STATES
                for j in self._jobs.values()
            ):
                raise MegaHelperError("Cancel active uploads before deleting this account", 409)
            remote = account["remote"]
            self._accounts.pop(account_id, None)
            self._quota_cache.pop(account_id, None)
            self._save_accounts_locked()
        self._delete_remote(remote)

    # ------------------------------------------------------------------
    # Upload planning and execution

    @staticmethod
    def _remote_folder(value: str) -> str:
        value = (value or "ReClip").strip().replace("\\", "/")
        parts = []
        for raw in value.split("/"):
            part = raw.strip()
            if not part:
                continue
            if part in {".", ".."} or any(ord(ch) < 32 for ch in part):
                raise MegaHelperError("Invalid MEGA destination folder")
            parts.append(part)
        folder = "/".join(parts)
        if not folder or len(folder) > 240:
            raise MegaHelperError("MEGA destination folder is required (maximum 240 characters)")
        return folder

    @staticmethod
    def _remote_component(value: str) -> str:
        value = str(value or "").replace("/", "-").replace("\\", "-").strip()
        value = "".join(ch for ch in value if ord(ch) >= 32)
        return value[:120]

    def _resolve_selection(self, selection: object) -> list[dict]:
        if not isinstance(selection, list) or not selection:
            raise MegaHelperError("Select at least one downloaded file")
        if len(selection) > 500:
            raise MegaHelperError("At most 500 files can be queued at once")
        clean = []
        seen = set()
        for value in selection:
            if not isinstance(value, dict):
                raise MegaHelperError("Invalid file selection")
            filename = str(value.get("filename") or "").strip()
            group_id = str(value.get("group_id") or "").strip()
            if not filename or os.path.basename(filename) != filename:
                raise MegaHelperError("Invalid file selection")
            key = (group_id, filename)
            if key not in seen:
                seen.add(key)
                clean.append({"filename": filename, "group_id": group_id})
        try:
            resolved = self.file_resolver(clean)
        except MegaHelperError:
            raise
        except (KeyError, ValueError) as exc:
            raise MegaHelperError(str(exc) or "A selected file no longer exists", 404) from exc
        if len(resolved) != len(clean):
            raise MegaHelperError("A selected file no longer exists", 404)
        result = []
        for item in resolved:
            path = os.path.realpath(str(item.get("path") or ""))
            try:
                if os.path.commonpath([self.download_dir, path]) != self.download_dir:
                    raise MegaHelperError("Invalid local file path")
                stat = os.stat(path)
            except (OSError, ValueError) as exc:
                raise MegaHelperError("A selected file no longer exists", 404) from exc
            if not os.path.isfile(path) or stat.st_size <= 0:
                raise MegaHelperError("Only completed, non-empty files can be uploaded")
            result.append(
                {
                    "path": path,
                    "filename": os.path.basename(path),
                    "size": stat.st_size,
                    "group_id": str(item.get("group_id") or ""),
                    "group_name": str(item.get("group_name") or ""),
                }
            )
        return result

    def enqueue(
        self,
        selection: object,
        *,
        account_id: str = "auto",
        folder: str = "ReClip",
        preserve_groups: bool = True,
    ) -> dict:
        # Keep source resolution and job registration atomic with ReClip's
        # library moves/deletes. Either the upload claims the old identity or a
        # concurrent request must resolve the new one.
        with self.operation_lock:
            return self._enqueue_locked(
                selection,
                account_id=account_id,
                folder=folder,
                preserve_groups=preserve_groups,
            )

    def _enqueue_locked(
        self,
        selection: object,
        *,
        account_id: str = "auto",
        folder: str = "ReClip",
        preserve_groups: bool = True,
    ) -> dict:
        self._ensure_available()
        files = self._resolve_selection(selection)
        folder = self._remote_folder(folder)
        # Allocation always starts with a fresh server-side storage quota.
        self.refresh_all_quotas(force=True)

        with self._lock:
            enabled = {
                account["id"]: account
                for account in self._accounts.values()
                if account.get("enabled", True)
                and not (self._quota_cache.get(account["id"]) or {}).get("error")
                and isinstance((self._quota_cache.get(account["id"]) or {}).get("free"), int)
            }
            if account_id != "auto":
                if account_id not in self._accounts:
                    raise MegaHelperError("MEGA account not found", 404)
                if account_id not in enabled:
                    raise MegaHelperError("The selected MEGA account is disabled or unavailable", 409)
                enabled = {account_id: enabled[account_id]}
            if not enabled:
                raise MegaHelperError("No enabled MEGA account with a readable quota is available", 409)

            reserved = self._reserved_locked()
            available = {
                aid: max(
                    0,
                    int(self._quota_cache[aid]["free"])
                    - reserved.get(aid, 0)
                    - self.safety_bytes,
                )
                for aid in enabled
            }
            assignment: dict[int, str] = {}
            # Largest-file-first and most-free account selection avoids a small
            # file consuming the only account capable of holding a large file.
            for index in sorted(range(len(files)), key=lambda i: files[i]["size"], reverse=True):
                size = files[index]["size"]
                candidates = [aid for aid, free in available.items() if free >= size]
                if not candidates:
                    largest = max(available.values(), default=0)
                    raise MegaHelperError(
                        f"Not enough MEGA storage for {files[index]['filename']} "
                        f"({size} bytes; largest available account has {largest} bytes)",
                        409,
                    )
                chosen = max(candidates, key=lambda aid: available[aid])
                assignment[index] = chosen
                available[chosen] -= size

            batch_id = uuid.uuid4().hex
            created = time.time()
            new_jobs = []
            for index, item in enumerate(files):
                aid = assignment[index]
                account = enabled[aid]
                remote_parts = [folder]
                if preserve_groups and item.get("group_name"):
                    component = self._remote_component(item["group_name"])
                    if component:
                        remote_parts.append(component)
                remote_parts.append(item["filename"])
                job_id = uuid.uuid4().hex
                job = {
                    "id": job_id,
                    "batch_id": batch_id,
                    "status": "queued",
                    "filename": item["filename"],
                    "size": item["size"],
                    "group_id": item.get("group_id") or "",
                    "account_id": aid,
                    "account_label": account["label"],
                    "remote_path": "/".join(remote_parts),
                    "progress": 0.0,
                    "uploaded_bytes": 0,
                    "speed": 0.0,
                    "eta": None,
                    "error": None,
                    "created_at": created,
                    "started_at": None,
                    "finished_at": None,
                    "cancel_requested": False,
                    "remote_uploaded": False,
                    "public_url": None,
                    "link_error": None,
                    "_source": item["path"],
                    "_remote": account["remote"],
                    "_process": None,
                    "_future": None,
                }
                self._jobs[job_id] = job
                new_jobs.append(job)
            self._prune_jobs_locked()
            self._save_jobs_locked()
            for job in new_jobs:
                job["_future"] = self._pool.submit(self._run_upload, job["id"])
            return {
                "batch_id": batch_id,
                "jobs": [self._public_job(job) for job in new_jobs],
            }

    @staticmethod
    def _public_job(job: dict) -> dict:
        return {k: v for k, v in job.items() if not k.startswith("_")}

    def public_jobs(self, limit: int = 100) -> list[dict]:
        with self._lock:
            ordered = sorted(
                self._jobs.values(), key=lambda item: item.get("created_at", 0), reverse=True
            )
            return [self._public_job(job) for job in ordered[: max(1, min(limit, 300))]]

    @staticmethod
    def _validated_public_url(output: str) -> str:
        lines = [line.strip() for line in (output or "").splitlines() if line.strip()]
        if not lines:
            raise MegaHelperError("MEGA did not return a public link", 502)
        value = lines[-1]
        if not is_mega_public_url(value):
            raise MegaHelperError("MEGA returned an invalid public link", 502)
        return value

    def _link_callback_payload(self, job: dict) -> dict:
        payload = self._public_job(job)
        payload["source_path"] = job.get("_source")
        return payload

    def _notify_link_ready(self, job: dict) -> None:
        if self.on_link_ready:
            self.on_link_ready(self._link_callback_payload(job))

    def reconcile_links(self) -> int:
        """Re-apply retained public links to the owning library after startup."""
        if not self.on_link_ready:
            return 0
        with self._lock:
            jobs = [dict(job) for job in self._jobs.values() if job.get("public_url")]
        restored = 0
        for job in jobs:
            try:
                self._notify_link_ready(job)
                restored += 1
            except Exception:
                # A missing historical library row must not prevent startup.
                continue
        return restored

    def has_active_upload(self, filename: str, group_id: str = "") -> bool:
        with self._lock:
            return any(
                job.get("status") in ACTIVE_STATES
                and (job.get("filename") or "") == filename
                and (job.get("group_id") or "") == (group_id or "")
                for job in self._jobs.values()
            )

    def _finish_link_error(self, job_id: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["status"] = "error"
            job["remote_uploaded"] = True
            job["link_error"] = message[:1000]
            job["error"] = message[:1000]
            job["finished_at"] = time.time()
            job["speed"] = 0.0
            job["eta"] = None
            self._save_jobs_locked()

    def create_public_link(self, job_id: str) -> dict:
        """Create and persist the public URL for an already-uploaded MEGA object."""
        self._ensure_available()
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise MegaHelperError("Upload not found", 404)
            status = job.get("status")
            if status in TRANSFER_STATES:
                raise MegaHelperError("Wait for the MEGA upload to finish", 409)
            if status == "cancelled" or not (
                job.get("remote_uploaded") or status == "done" or status == "linking"
            ):
                raise MegaHelperError("The file has not been uploaded to MEGA", 409)
            account = self._accounts.get(job.get("account_id"))
            remote = job.get("_remote") or (account or {}).get("remote")
            if not remote:
                raise MegaHelperError(
                    "The MEGA account is no longer configured, so its link cannot be created",
                    409,
                )
            destination = f"{remote}:{job['remote_path']}"
            job["status"] = "linking"
            job["error"] = None
            job["link_error"] = None
            job["finished_at"] = None
            self._save_jobs_locked()

        try:
            proc = self._rclone_command(
                [
                    "link",
                    destination,
                    "--config",
                    self.rclone_config,
                    "--contimeout",
                    "20s",
                    "--timeout",
                    "30s",
                ],
                timeout=60,
            )
        except MegaHelperError as exc:
            message = (
                "Upload completed, but the public link could not be created: "
                + exc.message
            )
            self._finish_link_error(job_id, message)
            raise MegaHelperError(message, exc.status) from exc
        if proc.returncode != 0:
            message = "Upload completed, but the public link could not be created: " + self._last_error(
                proc, "MEGA link command failed"
            )
            self._finish_link_error(job_id, message)
            raise MegaHelperError(message, 502)
        try:
            public_url = self._validated_public_url(proc.stdout)
        except MegaHelperError as exc:
            message = f"Upload completed, but the public link could not be created: {exc.message}"
            self._finish_link_error(job_id, message)
            raise MegaHelperError(message, exc.status) from exc

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise MegaHelperError("Upload not found", 404)
            job["public_url"] = public_url
            job["remote_uploaded"] = True
            job["uploaded_at"] = job.get("uploaded_at") or time.time()
            self._save_jobs_locked()
            callback_job = dict(job)

        try:
            self._notify_link_ready(callback_job)
        except Exception as exc:
            message = (
                "The MEGA link was created, but ReClip could not save it for the video: "
                + (str(exc) or exc.__class__.__name__)
            )
            self._finish_link_error(job_id, message)
            raise MegaHelperError(message, 500) from exc

        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise MegaHelperError("Upload not found", 404)
            job["status"] = "done"
            job["progress"] = 100.0
            job["error"] = None
            job["link_error"] = None
            job["finished_at"] = time.time()
            self._save_jobs_locked()
            return self._public_job(job)

    def _run_upload(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if job.get("cancel_requested"):
                job["status"] = "cancelled"
                job["finished_at"] = time.time()
                self._save_jobs_locked()
                return
            job["status"] = "uploading"
            job["started_at"] = time.time()
            source = job["_source"]
            destination = f"{job['_remote']}:{job['remote_path']}"
            self._save_jobs_locked()

        command = [
            self.rclone_bin,
            "copyto",
            source,
            destination,
            "--config",
            self.rclone_config,
            "--ignore-existing",
            "--use-json-log",
            "--log-level",
            "INFO",
            "--stats",
            "1s",
            "--stats-one-line",
            "--contimeout",
            "30s",
            "--timeout",
            "5m",
            "--retries",
            "3",
            "--low-level-retries",
            "10",
        ]
        popen_args = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "text": True,
            "bufsize": 1,
        }
        if os.name != "nt":
            popen_args["start_new_session"] = True
        errors: deque[str] = deque(maxlen=8)
        process = None
        try:
            process = subprocess.Popen(command, **popen_args)
            with self._lock:
                live = self._jobs.get(job_id)
                if not live:
                    return
                live["_process"] = process
                cancel_now = live.get("cancel_requested")
            if cancel_now:
                self._terminate_process(process)

            assert process.stdout is not None
            for line in process.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    errors.append(line[:500])
                    continue
                stats = event.get("stats") if isinstance(event, dict) else None
                if isinstance(stats, dict):
                    uploaded = int(stats.get("bytes") or 0)
                    total = int(stats.get("totalBytes") or 0)
                    speed = float(stats.get("speed") or 0)
                    eta = stats.get("eta")
                    with self._lock:
                        live = self._jobs.get(job_id)
                        if live:
                            live["uploaded_bytes"] = max(live.get("uploaded_bytes", 0), uploaded)
                            live["progress"] = round(
                                min(99.9, uploaded / (total or live["size"]) * 100), 1
                            )
                            live["speed"] = max(0.0, speed)
                            live["eta"] = int(eta) if isinstance(eta, (int, float)) else None
                if isinstance(event, dict) and event.get("level") in {"error", "critical"}:
                    errors.append(str(event.get("msg") or line)[:500])

            process.stdout.close()
            returncode = process.wait()
            should_link = False
            with self._lock:
                live = self._jobs.get(job_id)
                if not live:
                    return
                if live.get("cancel_requested"):
                    live["status"] = "cancelled"
                    live["error"] = None
                    live["finished_at"] = time.time()
                elif returncode == 0:
                    live["status"] = "linking"
                    live["progress"] = 100.0
                    live["remote_uploaded"] = True
                    live["uploaded_at"] = time.time()
                    # A sub-second upload may finish before the first stats tick.
                    if not live.get("uploaded_bytes"):
                        live["uploaded_bytes"] = live["size"]
                    live["error"] = None
                    live["finished_at"] = None
                    quota = self._quota_cache.get(live["account_id"])
                    if quota and isinstance(quota.get("free"), int):
                        moved = min(live["size"], int(live.get("uploaded_bytes") or 0))
                        quota["used"] = int(quota.get("used") or 0) + moved
                        quota["free"] = max(0, quota["free"] - moved)
                    should_link = True
                else:
                    live["status"] = "error"
                    live["error"] = "\n".join(errors) or f"rclone exited with code {returncode}"
                    live["finished_at"] = time.time()
                    self._quota_cache.pop(live["account_id"], None)
                live["speed"] = 0.0
                live["eta"] = None
                live["_process"] = None
                self._save_jobs_locked()
            if should_link:
                try:
                    self.create_public_link(job_id)
                except MegaHelperError:
                    # create_public_link persists a user-visible link error.
                    pass
        except FileNotFoundError:
            self._finish_with_error(job_id, "rclone is not installed")
        except Exception as exc:  # worker errors must become visible in the UI
            self._finish_with_error(job_id, str(exc) or exc.__class__.__name__)
        finally:
            if process and process.poll() is None:
                self._terminate_process(process)

    def _finish_with_error(self, job_id: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job["status"] = "cancelled" if job.get("cancel_requested") else "error"
            job["error"] = None if job["status"] == "cancelled" else message[:1000]
            job["finished_at"] = time.time()
            job["speed"] = 0.0
            job["eta"] = None
            job["_process"] = None
            self._save_jobs_locked()

    @staticmethod
    def _terminate_process(process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        try:
            if os.name != "nt":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                if os.name != "nt":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except OSError:
                pass

    def cancel(self, job_id: str) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise MegaHelperError("Upload not found", 404)
            if job.get("status") in FINAL_STATES:
                return self._public_job(job)
            if job.get("status") == "linking":
                raise MegaHelperError("The upload is already creating its MEGA link", 409)
            job["cancel_requested"] = True
            future = job.get("_future")
            process = job.get("_process")
            if job.get("status") == "queued" and future and future.cancel():
                job["status"] = "cancelled"
                job["finished_at"] = time.time()
                self._save_jobs_locked()
            public = self._public_job(job)
        if process:
            self._terminate_process(process)
        return public

    def clear_finished(self) -> int:
        with self._lock:
            ids = [job_id for job_id, job in self._jobs.items() if job.get("status") in FINAL_STATES]
            for job_id in ids:
                self._jobs.pop(job_id, None)
            self._save_jobs_locked()
            return len(ids)

    # ------------------------------------------------------------------
    # Flask API

    def _register_routes(self) -> None:
        bp = self.blueprint

        @bp.errorhandler(MegaHelperError)
        def handle_error(exc: MegaHelperError):
            return jsonify({"error": exc.message}), exc.status

        @bp.route("/accounts")
        def list_accounts():
            refresh = request.args.get("refresh") in {"1", "true", "yes"}
            return jsonify(
                {
                    **self.availability(force=refresh),
                    "max_concurrent": self.max_workers,
                    "quota_kind": "storage",
                    "accounts": self.public_accounts(refresh=refresh),
                }
            )

        @bp.route("/accounts", methods=["POST"])
        def create_account():
            data = request.get_json(silent=True) or {}
            account = self.add_account(
                str(data.get("label") or ""),
                str(data.get("email") or ""),
                str(data.get("password") or ""),
            )
            return jsonify({"account": account}), 201

        @bp.route("/accounts/<account_id>", methods=["PATCH"])
        def patch_account(account_id: str):
            if not ACCOUNT_ID_RE.match(account_id):
                raise MegaHelperError("Invalid MEGA account id")
            data = request.get_json(silent=True) or {}
            return jsonify({"account": self.update_account(account_id, data)})

        @bp.route("/accounts/<account_id>", methods=["DELETE"])
        def remove_account(account_id: str):
            if not ACCOUNT_ID_RE.match(account_id):
                raise MegaHelperError("Invalid MEGA account id")
            self.delete_account(account_id)
            return jsonify({"ok": True})

        @bp.route("/uploads")
        def list_uploads():
            try:
                limit = int(request.args.get("limit", "100"))
            except ValueError:
                limit = 100
            return jsonify({"uploads": self.public_jobs(limit)})

        @bp.route("/uploads", methods=["POST"])
        def create_uploads():
            data = request.get_json(silent=True) or {}
            result = self.enqueue(
                data.get("files"),
                account_id=str(data.get("account_id") or "auto"),
                folder=str(data.get("folder") or "ReClip"),
                preserve_groups=bool(data.get("preserve_groups", True)),
            )
            return jsonify(result), 202

        @bp.route("/uploads/<job_id>")
        def get_upload(job_id: str):
            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    raise MegaHelperError("Upload not found", 404)
                return jsonify(self._public_job(job))

        @bp.route("/uploads/<job_id>/cancel", methods=["POST"])
        def cancel_upload(job_id: str):
            return jsonify(self.cancel(job_id))

        @bp.route("/uploads/<job_id>/link", methods=["POST"])
        def create_upload_link(job_id: str):
            return jsonify(self.create_public_link(job_id))

        @bp.route("/uploads/finished", methods=["DELETE"])
        def clear_uploads():
            return jsonify({"ok": True, "removed": self.clear_finished()})
