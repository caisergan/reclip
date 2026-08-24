import copy
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch


_TEST_ROOT = tempfile.TemporaryDirectory()
_DOWNLOAD_DIR = Path(_TEST_ROOT.name) / "downloads"
_DOWNLOAD_DIR.mkdir()
os.environ["RECLIP_DOWNLOAD_DIR"] = str(_DOWNLOAD_DIR)
os.environ["RECLIP_DB"] = str(_DOWNLOAD_DIR / "reclip.db")
os.environ["RECLIP_MEGA_DIR"] = str(_DOWNLOAD_DIR / ".mega")
os.environ["RECLIP_RCLONE"] = "/bin/false"

import app as reclip  # noqa: E402


class ReClipApiTests(unittest.TestCase):
    def setUp(self):
        reclip.app.config.update(TESTING=True)
        self.client = reclip.app.test_client()
        reclip.jobs.clear()
        reclip.batches.clear()
        with reclip.fetch_lock:
            reclip.fetch_batches.clear()
        with reclip._fetch_info_lock:
            reclip._fetch_info_cache.clear()
        with reclip.index_lock:
            reclip.download_index.clear()
        with reclip.groups_lock:
            reclip.groups.clear()
        with reclip.mega_helper._lock:
            reclip.mega_helper._jobs.clear()
            reclip.mega_helper._save_jobs_locked()
        with reclip._db_lock:
            conn = reclip._db_connect()
            try:
                with conn:
                    conn.execute("DELETE FROM downloads")
                    conn.execute("DELETE FROM groups")
                    conn.execute("DELETE FROM fetch_batches")
            finally:
                conn.close()
        for path in _DOWNLOAD_DIR.iterdir():
            if path.name in {"reclip.db", "reclip.db-wal", "reclip.db-shm", ".mega"}:
                continue
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

    def add_group(self, gid, name=None):
        with reclip.groups_lock:
            reclip.groups[gid] = {"name": name or gid.title(), "created_at": time.time()}
            reclip._save_groups_locked()
        folder = _DOWNLOAD_DIR / gid
        folder.mkdir(exist_ok=True)
        return folder

    def add_download(self, name, group_id="", *, video_id=None, content=b"video"):
        folder = self.add_group(group_id) if group_id and group_id not in reclip.groups else (
            _DOWNLOAD_DIR / group_id if group_id else _DOWNLOAD_DIR
        )
        folder.mkdir(exist_ok=True)
        path = folder / name
        path.write_bytes(content)
        token = video_id or f"{group_id}-{name}"
        reclip.register_download(
            token, "youtube", name, f"https://example.test/{token}",
            "video", "best", str(path), group_id=group_id,
        )
        return path

    def wait_for_fetch(self, batch_id, timeout=3):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with reclip.fetch_lock:
                batch = reclip.fetch_batches.get(batch_id)
                if batch and batch["finished"]:
                    return {
                        **batch,
                        "urls": [dict(item) for item in batch["urls"]],
                    }
            time.sleep(0.01)
        self.fail("fetch batch did not finish")

    def test_overhaul_is_index_and_legacy_ui_remains_available(self):
        root = self.client.get("/")
        self.assertEqual(root.status_code, 200)
        self.assertIn(b"<title>ReClip</title>", root.data)
        self.assertIn(b'id="megaOverlay"', root.data)
        self.assertIn(b'id="librarySelectAllBtn"', root.data)
        self.assertIn(b'id="splitFetch"', root.data)
        self.assertIn(b'id="fetchProgress"', root.data)
        self.assertIn(b'id="selectAllBtn"', root.data)
        self.assertIn(b'id="pauseFetchBtn"', root.data)
        self.assertIn(b'id="stopFetchBtn"', root.data)
        self.assertIn(b'id="fetchConcSel"', root.data)
        self.assertIn(b"toggleCardSelection", root.data)
        self.assertNotIn(b'class="card-check"', root.data)
        self.assertNotIn(b'id="selectAll"', root.data)
        self.assertIn(b'id="libraryMoveBtn"', root.data)

        legacy = self.client.get("/legacy")
        self.assertEqual(legacy.status_code, 200)
        self.assertIn(b"Free Media Downloader", legacy.data)
        self.assertIn(b'id="libraryMoveTarget"', legacy.data)
        self.assertNotIn(b"fetch('/api/playlist'", legacy.data)

    def test_fetch_batch_can_be_reattached_and_finishes_with_normalized_info(self):
        entered = threading.Event()
        release = threading.Event()

        def fake_info(url):
            entered.set()
            self.assertTrue(release.wait(2))
            return {
                "title": "Controlled fetch",
                "thumbnail": "",
                "duration": 75,
                "uploader": "Tests",
                "formats": [{"id": "1080", "label": "1080p", "height": 1080}],
                "id": "controlled-1",
                "extractor": "test",
                "already_on_server": False,
                "existing_file": "",
            }

        with patch.object(reclip, "fetch_video_info", side_effect=fake_info):
            started = self.client.post(
                "/api/fetch", json={"urls": ["https://example.test/video"], "mode": "video"}
            )
            self.assertEqual(started.status_code, 200)
            batch_id = started.get_json()["batch_id"]
            self.assertTrue(entered.wait(2))

            library = self.client.get("/api/library").get_json()
            self.assertIn(batch_id, library["fetch_batches"])

            release.set()
            deadline = time.time() + 3
            state = None
            while time.time() < deadline:
                state = self.client.get(f"/api/fetch/{batch_id}").get_json()
                if state.get("finished"):
                    break
                time.sleep(0.02)

        self.assertTrue(state["finished"])
        self.assertEqual(state["urls"][0]["status"], "done")
        self.assertEqual(state["urls"][0]["title"], "Controlled fetch")
        self.assertEqual(state["urls"][0]["formats"][0]["id"], "1080")

    def test_fetch_batch_pause_holds_queued_items_until_resume(self):
        original_limit = reclip.fetch_gate.limit
        entered = threading.Event()
        release = threading.Event()
        calls = 0
        calls_lock = threading.Lock()

        def fake_info(url):
            nonlocal calls
            with calls_lock:
                calls += 1
                call_number = calls
            if call_number == 1:
                entered.set()
                self.assertTrue(release.wait(3))
            return {
                "title": url, "thumbnail": "", "duration": None,
                "uploader": "Tests", "formats": [], "id": url,
                "extractor": "test", "already_on_server": False,
                "existing_file": "",
            }

        batch_id = None
        reclip.fetch_gate.set_limit(1)
        try:
            with patch.object(reclip, "fetch_video_info", side_effect=fake_info):
                started = self.client.post("/api/fetch", json={
                    "urls": [f"https://example.test/pause-{index}" for index in range(3)],
                    "mode": "video", "fetch_concurrent": 1,
                })
                batch_id = started.get_json()["batch_id"]
                self.assertTrue(entered.wait(2))

                paused = self.client.post(
                    f"/api/fetch/{batch_id}/pause", json={"paused": True}
                ).get_json()
                self.assertTrue(paused["paused"])
                self.assertNotIn("_futures", paused)

                release.set()
                deadline = time.time() + 2
                while time.time() < deadline:
                    paused = self.client.get(f"/api/fetch/{batch_id}").get_json()
                    if sum(item["status"] == "done" for item in paused["urls"]) == 1:
                        break
                    time.sleep(0.01)
                time.sleep(0.05)
                paused = self.client.get(f"/api/fetch/{batch_id}").get_json()
                self.assertFalse(paused["finished"])
                self.assertEqual(calls, 1)
                self.assertEqual(
                    [item["status"] for item in paused["urls"]].count("loading"), 2
                )

                resumed = self.client.post(
                    f"/api/fetch/{batch_id}/pause", json={"paused": False}
                ).get_json()
                self.assertFalse(resumed["paused"])
                finished = self.wait_for_fetch(batch_id)
                self.assertTrue(all(item["status"] == "done" for item in finished["urls"]))
                self.assertEqual(calls, 3)
        finally:
            release.set()
            if batch_id:
                self.client.post(f"/api/fetch/{batch_id}/pause", json={"paused": False})
            reclip.fetch_gate.set_limit(original_limit)
            reclip.fetch_gate.wake_all()

    def test_stop_fetch_batch_cancels_unfinished_items_immediately(self):
        original_limit = reclip.fetch_gate.limit
        entered = threading.Event()
        release = threading.Event()

        def fake_info(url):
            entered.set()
            self.assertTrue(release.wait(3))
            return {
                "title": url, "thumbnail": "", "duration": None,
                "uploader": "Tests", "formats": [], "id": url,
                "extractor": "test", "already_on_server": False,
                "existing_file": "",
            }

        batch_id = None
        reclip.fetch_gate.set_limit(1)
        try:
            with patch.object(reclip, "fetch_video_info", side_effect=fake_info):
                started = self.client.post("/api/fetch", json={
                    "urls": [f"https://example.test/stop-{index}" for index in range(3)],
                    "mode": "video", "fetch_concurrent": 1,
                })
                batch_id = started.get_json()["batch_id"]
                self.assertTrue(entered.wait(2))

                stopped = self.client.post(
                    f"/api/fetch/{batch_id}/stop"
                ).get_json()
                self.assertTrue(stopped["finished"])
                self.assertTrue(stopped["stopped"])
                self.assertFalse(stopped["paused"])
                self.assertTrue(all(
                    item["status"] == "cancelled" for item in stopped["urls"]
                ))
                self.assertNotIn(
                    batch_id, self.client.get("/api/fetch/list").get_json()["batches"]
                )

                release.set()
                deadline = time.time() + 2
                while time.time() < deadline and reclip.fetch_gate.active:
                    time.sleep(0.01)
                final = self.client.get(f"/api/fetch/{batch_id}").get_json()
                self.assertTrue(all(
                    item["status"] == "cancelled" for item in final["urls"]
                ))
        finally:
            release.set()
            reclip.fetch_gate.set_limit(original_limit)
            reclip.fetch_gate.wake_all()

    def test_fetch_info_coalesces_duplicates_caches_and_refreshes_library_state(self):
        url = "https://example.test/cache-target"
        entered = threading.Event()
        release = threading.Event()
        calls = 0
        calls_lock = threading.Lock()
        extracted = {
            "title": "Cached metadata",
            "thumbnail": "",
            "duration": 12,
            "uploader": "Tests",
            "formats": [{"id": "720", "label": "720p", "height": 720}],
            "id": "cache-target",
            "extractor": "test",
        }

        def fake_extract(requested_url):
            nonlocal calls
            self.assertEqual(requested_url, url)
            with calls_lock:
                calls += 1
            entered.set()
            self.assertTrue(release.wait(2))
            return extracted

        with patch.object(reclip, "_extract_video_info", side_effect=fake_extract):
            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = [pool.submit(reclip.fetch_video_info, url) for _ in range(6)]
                self.assertTrue(entered.wait(2))
                release.set()
                results = [future.result(timeout=2) for future in futures]

            self.assertEqual(calls, 1)
            self.assertTrue(all(not item["already_on_server"] for item in results))
            # Returned cards cannot mutate the cached nested format list.
            results[0]["formats"][0]["label"] = "changed"
            self.assertEqual(reclip.fetch_video_info(url)["formats"][0]["label"], "720p")

            media = _DOWNLOAD_DIR / "cache-target.mp4"
            media.write_bytes(b"cached video")
            reclip.register_download(
                "cache-target", "test", "Cached metadata", url,
                "video", "720", str(media),
            )
            refreshed = reclip.fetch_video_info(url)

        self.assertEqual(calls, 1, "library changes must not invalidate static metadata")
        self.assertTrue(refreshed["already_on_server"])
        self.assertEqual(refreshed["existing_file"], media.name)

    def test_provider_fixed_file_skips_known_failing_ytdlp_wrapper(self):
        resolved = {
            "media_url": "https://cdn.example.test/signed/file",
            "referer": "https://files.example.test/",
            "title": "archive.zip",
            "thumbnail": "",
            "extractor": "test-files",
            "filename": "archive.zip",
        }
        with patch.object(reclip, "_provider_resolver_applies", return_value=True), \
             patch.object(reclip, "resolve_provider", return_value=resolved), \
             patch.object(reclip, "run_ytdlp_json") as ytdlp:
            info = reclip._extract_video_info("https://files.example.test/f/one")

        ytdlp.assert_not_called()
        self.assertEqual(info["title"], "archive.zip")
        self.assertEqual(info["formats"][0]["id"], "direct")

    def test_candidate_probes_are_bounded_concurrent_and_ordered(self):
        candidates = [f"https://cdn.example.test/{index}.mp4" for index in range(10)]
        active = 0
        max_active = 0
        counter_lock = threading.Lock()

        def fake_verdict(url, referer=None):
            nonlocal active, max_active
            with counter_lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.02)
                return int(url.rsplit("/", 1)[1].split(".", 1)[0]) % 3 - 1
            finally:
                with counter_lock:
                    active -= 1

        with patch.object(reclip, "media_verdict", side_effect=fake_verdict):
            results = list(reclip._ordered_media_verdicts(candidates, referer="ref"))

        self.assertEqual([candidate for candidate, _ in results], candidates)
        self.assertGreater(max_active, 1)
        self.assertLessEqual(max_active, reclip.FETCH_PROBE_WINDOW)

    def test_playlist_roots_expand_concurrently_but_keep_input_order(self):
        roots = [f"https://example.test/watch?list={index}" for index in range(6)]
        active = 0
        max_active = 0
        counter_lock = threading.Lock()

        def fake_expand(url):
            nonlocal active, max_active
            with counter_lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.03)
                return [url + "/first", url + "/second"]
            finally:
                with counter_lock:
                    active -= 1

        with patch.object(reclip, "_expand_playlist_urls", side_effect=fake_expand):
            expanded = reclip._expand_fetch_urls(roots)

        self.assertGreater(max_active, 1)
        self.assertLessEqual(max_active, reclip.FETCH_EXPAND_WORKERS)
        self.assertEqual(
            expanded,
            [child for root in roots for child in (root + "/first", root + "/second")],
        )

    def test_provider_album_frontier_expands_concurrently_in_stable_order(self):
        root = "https://example.test/root"
        albums = [f"https://example.test/album/{index}" for index in range(8)]
        active = 0
        max_active = 0
        counter_lock = threading.Lock()
        provider = {"kind": "test-expander"}

        def matching(url):
            return provider if url == root or url in albums else None

        def expand(url, _provider):
            nonlocal active, max_active
            if url == root:
                return albums
            with counter_lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.02)
                return [url + "/file.mp4"]
            finally:
                with counter_lock:
                    active -= 1

        with patch.object(reclip, "_matching_expander", side_effect=matching), \
             patch.dict(reclip._PROVIDER_EXPANDERS, {"test-expander": expand}):
            expanded = reclip._expand_via_providers(root)

        self.assertEqual(expanded, [album + "/file.mp4" for album in albums])
        self.assertGreater(max_active, 1)
        self.assertLessEqual(max_active, reclip.FETCH_EXPAND_WORKERS)

    def test_fetch_batch_persists_only_initial_and_final_checkpoints(self):
        urls = [f"https://example.test/persist-{index}" for index in range(12)]
        writes = []

        def fake_info(url):
            return {
                "title": url,
                "thumbnail": "",
                "duration": None,
                "uploader": "",
                "formats": [],
                "id": None,
                "extractor": "test",
                "already_on_server": False,
                "existing_file": "",
            }

        with patch.object(reclip, "fetch_video_info", side_effect=fake_info), \
             patch.object(
                 reclip, "_persist_fetch_batch",
                 side_effect=lambda batch: writes.append(copy.deepcopy(batch)),
             ):
            batch_id = reclip._start_fetch_batch(urls)
            state = self.wait_for_fetch(batch_id)

        self.assertTrue(state["finished"])
        self.assertEqual(len(writes), 2)
        self.assertFalse(writes[0]["finished"])
        self.assertTrue(writes[1]["finished"])
        self.assertTrue(all(item["status"] == "done" for item in writes[1]["urls"]))

    def test_concurrent_scrape_expansion_tracks_stable_items_not_shifted_indexes(self):
        first = "https://example.test/scrape-first"
        second = "https://example.test/scrape-second"
        second_started = threading.Event()
        first_applied = threading.Event()
        real_complete = reclip._complete_fetch_item

        def fake_scrape(url):
            if url == first:
                self.assertTrue(second_started.wait(2))
                return [first + "/a.mp4", first + "/b.mp4"]
            second_started.set()
            self.assertTrue(first_applied.wait(2))
            return [second + "/c.mp4"]

        def tracked_complete(batch_id, target, **kwargs):
            result = real_complete(batch_id, target, **kwargs)
            if target["url"] == first:
                first_applied.set()
            return result

        with patch.object(reclip, "_scrape_page", side_effect=fake_scrape), \
             patch.object(reclip, "_complete_fetch_item", side_effect=tracked_complete):
            batch_id = reclip._start_fetch_batch([first, second], kind="scrape")
            state = self.wait_for_fetch(batch_id)

        self.assertEqual(
            [item["url"] for item in state["urls"]],
            [first + "/a.mp4", first + "/b.mp4", second + "/c.mp4"],
        )
        self.assertTrue(all(item["status"] == "done" for item in state["urls"]))

    def test_download_dedup_reuses_library_file_and_protects_shared_reference(self):
        existing = _DOWNLOAD_DIR / "existing.mp4"
        existing.write_bytes(b"already downloaded")
        reclip.register_download(
            "video-1",
            "youtube",
            "Existing",
            "https://example.test/watch/1",
            "video",
            "1080",
            str(existing),
        )
        item = {
            "url": "https://example.test/watch/1",
            "title": "Existing",
            "format": "video",
            "format_id": "1080",
            "id": "video-1",
            "extractor": "youtube",
            "thumbnail": "",
            "group_id": "",
        }

        first_id = reclip.launch_job(item)
        second_id = reclip.launch_job(item)
        first = reclip.jobs[first_id]
        second = reclip.jobs[second_id]

        self.assertEqual(first["status"], "done")
        self.assertTrue(first["deduped"])
        self.assertEqual(first["file"], str(existing))
        self.assertEqual(second["file"], str(existing))

        reclip.delete_job(first_id)
        self.assertTrue(existing.exists(), "deleting one shared job must retain the library file")
        self.assertIsNotNone(
            reclip.find_existing_download(
                "video-1", "youtube", item["url"], "video", "1080"
            )
        )

        reclip.delete_job(second_id)
        self.assertFalse(existing.exists())
        self.assertIsNone(
            reclip.find_existing_download(
                "video-1", "youtube", item["url"], "video", "1080"
            )
        )

    def test_mega_link_survives_local_delete_transfer_clear_and_reload(self):
        media = _DOWNLOAD_DIR / "linked.mp4"
        media.write_bytes(b"linked video bytes")
        thumb = _DOWNLOAD_DIR / "linked.jpg"
        thumb.write_bytes(b"thumbnail")
        reclip.register_download(
            "linked-1",
            "youtube",
            "Linked video",
            "https://example.test/watch/linked",
            "video",
            "1080",
            str(media),
            duration=42,
            thumb=str(thumb),
            group_id="",
        )
        # rclone may return MEGA's legacy but still official mega.co.nz host.
        mega_url = "https://mega.co.nz/file/node-id#decryption-key"
        reclip._record_mega_link({
            "id": "mega-job-1",
            "filename": media.name,
            "group_id": "",
            "source_path": str(media),
            "size": media.stat().st_size,
            "public_url": mega_url,
            "remote_path": "ReClip/linked.mp4",
            "account_id": "primary-account",
            "account_label": "Primary",
            "uploaded_at": time.time(),
        })
        with reclip.mega_helper._lock:
            reclip.mega_helper._jobs["mega-job-1"] = {
                "id": "mega-job-1", "status": "done", "created_at": time.time()
            }

        before = self.client.get("/api/library").get_json()["files"][0]
        self.assertTrue(before["local_available"])
        self.assertEqual(before["mega_url"], mega_url)
        self.assertTrue(before["has_thumb"])

        cleared = self.client.delete("/api/mega/uploads/finished")
        self.assertEqual(cleared.status_code, 200)
        self.assertEqual(
            self.client.get("/api/library").get_json()["files"][0]["mega_url"],
            mega_url,
        )

        deleted = self.client.post(
            "/api/library/delete", json={"f": media.name, "group_id": ""}
        )
        self.assertEqual(deleted.status_code, 200)
        self.assertTrue(deleted.get_json()["record_retained"])
        self.assertFalse(media.exists())
        self.assertTrue(thumb.exists())

        reclip.load_index()
        remote = self.client.get("/api/library").get_json()["files"][0]
        self.assertFalse(remote["local_available"])
        self.assertEqual(remote["mega_url"], mega_url)
        self.assertEqual(remote["size"], len(b"linked video bytes"))
        self.assertTrue(remote["has_thumb"])
        self.assertIsNotNone(remote["local_deleted_at"])
        self.assertIsNone(reclip.find_existing_download(
            "linked-1", "youtube", "https://example.test/watch/linked", "video", "1080"
        ))
        self.assertTrue(reclip.download_index, "remote-only dedup rows must not be pruned")
        with self.assertRaises(reclip.MegaHelperError):
            reclip._resolve_mega_files([{"filename": media.name, "group_id": ""}])

    def test_unlinked_library_delete_still_removes_the_record(self):
        media = _DOWNLOAD_DIR / "local-only.mp4"
        media.write_bytes(b"local only")
        reclip.register_download(
            "local-1", "youtube", "Local", "https://example.test/local",
            "video", "720", str(media),
        )
        response = self.client.post(
            "/api/library/delete", json={"f": media.name, "group_id": ""}
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["record_retained"])
        self.assertFalse(media.exists())
        self.assertEqual(reclip.download_index, {})
        self.assertEqual(self.client.get("/api/library").get_json()["files"], [])

    def test_library_delete_is_group_specific_and_blocks_active_mega_upload(self):
        first_dir = _DOWNLOAD_DIR / "group-a"
        second_dir = _DOWNLOAD_DIR / "group-b"
        first_dir.mkdir()
        second_dir.mkdir()
        first = first_dir / "same.mp4"
        second = second_dir / "same.mp4"
        first.write_bytes(b"first")
        second.write_bytes(b"second")
        reclip.register_download(
            "same-a", "youtube", "First", "https://example.test/a",
            "video", "best", str(first), group_id="group-a",
        )
        reclip.register_download(
            "same-b", "youtube", "Second", "https://example.test/b",
            "video", "best", str(second), group_id="group-b",
        )
        with patch.object(reclip.mega_helper, "has_active_upload", return_value=True):
            blocked = self.client.post(
                "/api/library/delete", json={"f": "same.mp4", "group_id": "group-a"}
            )
        self.assertEqual(blocked.status_code, 409)
        self.assertTrue(first.exists())

        deleted = self.client.post(
            "/api/library/delete", json={"f": "same.mp4", "group_id": "group-a"}
        )
        self.assertEqual(deleted.status_code, 200)
        self.assertFalse(first.exists())
        self.assertTrue(second.exists())
        files = self.client.get("/api/library").get_json()["files"]
        self.assertEqual([(item["group_id"], item["filename"]) for item in files],
                         [("group-b", "same.mp4")])

    def test_download_schema_contains_persistent_mega_fields(self):
        with reclip._db_lock:
            conn = reclip._db_connect()
            try:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(downloads)")}
            finally:
                conn.close()
        expected = {
            "size_bytes", "local_deleted_at", "mega_url", "mega_remote_path",
            "mega_account_id", "mega_account_label", "mega_uploaded_at",
        }
        self.assertTrue(expected.issubset(columns))

        old_db = _DOWNLOAD_DIR / "old-schema.db"
        conn = reclip.sqlite3.connect(old_db)
        try:
            conn.execute(
                "CREATE TABLE downloads (key TEXT PRIMARY KEY, file TEXT NOT NULL, "
                "filename TEXT, title TEXT, url TEXT, extractor TEXT, video_id TEXT, "
                "created_at REAL)"
            )
            conn.commit()
        finally:
            conn.close()
        with patch.object(reclip, "DB_PATH", str(old_db)):
            reclip._init_db()
        conn = reclip.sqlite3.connect(old_db)
        try:
            migrated = {row[1] for row in conn.execute("PRAGMA table_info(downloads)")}
        finally:
            conn.close()
        self.assertTrue(expected.issubset(migrated))

    def test_remote_only_item_becomes_ungrouped_when_group_is_deleted(self):
        gid = "archive"
        folder = _DOWNLOAD_DIR / gid
        folder.mkdir()
        media = folder / "remote.mp4"
        media.write_bytes(b"remote")
        with reclip.groups_lock:
            reclip.groups[gid] = {"name": "Archive", "created_at": time.time()}
            reclip._save_groups_locked()
        reclip.register_download(
            "remote-group", "youtube", "Remote", "https://example.test/remote",
            "video", "best", str(media), group_id=gid,
        )
        reclip._record_mega_link({
            "filename": media.name,
            "group_id": gid,
            "source_path": str(media),
            "size": media.stat().st_size,
            "public_url": "https://mega.nz/file/group-node#group-key",
            "remote_path": "ReClip/Archive/remote.mp4",
            "account_id": "primary",
            "account_label": "Primary",
            "uploaded_at": time.time(),
        })
        self.client.post("/api/library/delete", json={"f": media.name, "group_id": gid})
        response = self.client.delete(f"/api/groups/{gid}")
        self.assertEqual(response.status_code, 200)
        item = self.client.get("/api/library").get_json()["files"][0]
        self.assertEqual(item["group_id"], "")
        self.assertFalse(item["local_available"])
        self.assertTrue(item["mega_url"].startswith("https://mega.nz/"))

    def test_bulk_group_move_handles_collisions_dedup_rows_jobs_and_reload(self):
        source = self.add_group("source", "Source")
        target = self.add_group("archive", "Archive")
        existing = target / "clip.mp4"
        existing.write_bytes(b"existing target")
        reclip.register_download(
            "existing", "youtube", "Existing", "https://example.test/existing",
            "video", "best", str(existing), group_id="archive",
        )
        clip = source / "clip.mp4"
        clip.write_bytes(b"moving clip")
        reclip.register_download(
            "clip", "youtube", "Clip", "https://example.test/clip",
            "video", "best", str(clip), group_id="source",
        )
        other = self.add_download("other.mp4", content=b"other")
        reclip.jobs["done-clip"] = {
            "status": "done", "file": str(clip), "filename": "clip.mp4",
            "group_id": "source",
        }

        response = self.client.patch("/api/library/group", json={
            "files": [
                {"filename": "clip.mp4", "group_id": "source"},
                {"filename": "other.mp4", "group_id": ""},
                {"filename": "other.mp4", "group_id": ""},
            ],
            "target_group_id": "archive",
        })
        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(result["moved_count"], 2)
        self.assertEqual(result["unchanged_count"], 0)
        self.assertEqual(result["files"][0]["to"]["filename"], "clip (2).mp4")
        moved_clip = target / "clip (2).mp4"
        moved_other = target / "other.mp4"
        self.assertEqual(existing.read_bytes(), b"existing target")
        self.assertEqual(moved_clip.read_bytes(), b"moving clip")
        self.assertEqual(moved_other.read_bytes(), b"other")
        self.assertFalse(clip.exists())
        self.assertFalse(other.exists())
        self.assertEqual(reclip.jobs["done-clip"]["file"], str(moved_clip))
        self.assertEqual(reclip.jobs["done-clip"]["group_id"], "archive")
        clip_rows = [entry for entry in reclip.download_index.values()
                     if entry.get("video_id") == "clip"]
        self.assertGreaterEqual(len(clip_rows), 2)
        self.assertTrue(all(entry["file"] == str(moved_clip) for entry in clip_rows))

        reclip.load_index()
        files = self.client.get("/api/library").get_json()["files"]
        identities = {(item["group_id"], item["filename"]) for item in files}
        self.assertIn(("archive", "clip.mp4"), identities)
        self.assertIn(("archive", "clip (2).mp4"), identities)
        self.assertIn(("archive", "other.mp4"), identities)

    def test_group_move_is_atomic_on_validation_and_rename_failure(self):
        first = self.add_download("first.mp4")
        second = self.add_download("second.mp4")
        self.add_group("target", "Target")

        missing = self.client.patch("/api/library/group", json={
            "files": [
                {"filename": "first.mp4", "group_id": ""},
                {"filename": "missing.mp4", "group_id": ""},
            ],
            "target_group_id": "target",
        })
        self.assertEqual(missing.status_code, 404)
        self.assertTrue(first.exists())

        real_rename = os.rename

        def fail_second(source, destination):
            if source == str(second):
                raise OSError("controlled failure")
            return real_rename(source, destination)

        with patch.object(reclip.os, "rename", side_effect=fail_second):
            failed = self.client.patch("/api/library/group", json={
                "files": [
                    {"filename": "first.mp4", "group_id": ""},
                    {"filename": "second.mp4", "group_id": ""},
                ],
                "target_group_id": "target",
            })
        self.assertEqual(failed.status_code, 500)
        self.assertTrue(first.exists())
        self.assertTrue(second.exists())
        self.assertFalse((_DOWNLOAD_DIR / "target" / "first.mp4").exists())
        self.assertTrue(all(not entry.get("group_id")
                            for entry in reclip.download_index.values()))

    def test_group_move_validates_target_noop_and_active_mega_upload(self):
        source = self.add_download("source.mp4", "source")
        missing_target = self.client.patch("/api/library/group", json={
            "files": [{"filename": "source.mp4", "group_id": "source"}],
            "target_group_id": "missing",
        })
        self.assertEqual(missing_target.status_code, 404)

        noop = self.client.patch("/api/library/group", json={
            "files": [{"filename": "source.mp4", "group_id": "source"}],
            "target_group_id": "source",
        })
        self.assertEqual(noop.status_code, 200)
        self.assertEqual(noop.get_json()["unchanged_count"], 1)

        self.add_group("target")
        with patch.object(reclip.mega_helper, "has_active_upload", return_value=True):
            blocked = self.client.patch("/api/library/group", json={
                "files": [{"filename": "source.mp4", "group_id": "source"}],
                "target_group_id": "target",
            })
        self.assertEqual(blocked.status_code, 409)
        self.assertTrue(source.exists())

        malformed = self.client.patch("/api/library/group", json={
            "files": [{"filename": "source.mp4"}], "target_group_id": "target",
        })
        self.assertEqual(malformed.status_code, 400)

    def test_remote_only_item_can_change_group_without_changing_mega_location(self):
        media = self.add_download("remote.mp4", "source")
        self.add_group("target")
        mega_url = "https://mega.nz/file/remote-node#remote-key"
        remote_path = "ReClip/Source/remote.mp4"
        reclip._record_mega_link({
            "filename": media.name, "group_id": "source", "source_path": str(media),
            "size": media.stat().st_size, "public_url": mega_url,
            "remote_path": remote_path, "account_id": "primary",
            "account_label": "Primary", "uploaded_at": time.time(),
        })
        deleted = self.client.post(
            "/api/library/delete", json={"f": media.name, "group_id": "source"}
        )
        self.assertEqual(deleted.status_code, 200)

        moved = self.client.patch("/api/library/group", json={
            "files": [{"filename": "remote.mp4", "group_id": "source"}],
            "target_group_id": "target",
        })
        self.assertEqual(moved.status_code, 200)
        reclip.load_index()
        item = self.client.get("/api/library").get_json()["files"][0]
        self.assertEqual(item["group_id"], "target")
        self.assertFalse(item["local_available"])
        self.assertEqual(item["mega_url"], mega_url)
        self.assertEqual(item["mega_remote_path"], remote_path)

    def test_group_delete_uses_collision_safe_move(self):
        existing = self.add_download("same.mp4", content=b"existing")
        grouped = self.add_download("same.mp4", "source", content=b"grouped")
        response = self.client.delete("/api/groups/source")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["moved_count"], 1)
        renamed = _DOWNLOAD_DIR / "same (2).mp4"
        self.assertEqual(existing.read_bytes(), b"existing")
        self.assertEqual(renamed.read_bytes(), b"grouped")
        self.assertFalse(grouped.exists())
        self.assertNotIn("source", reclip.groups)

    def test_config_endpoint_updates_concurrency(self):
        original = reclip.gate.limit
        original_fetch = reclip.fetch_gate.limit
        try:
            response = self.client.post(
                "/api/config", json={"max_concurrent": 4, "fetch_concurrent": 8}
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["max_concurrent"], 4)
            self.assertEqual(response.get_json()["fetch_concurrent"], 8)
            config = self.client.get("/api/config").get_json()
            self.assertEqual(config["max_concurrent"], 4)
            self.assertEqual(config["fetch_concurrent"], 8)
            self.assertEqual(config["fetch_max_pool"], reclip.FETCH_MAX_WORKERS)
        finally:
            reclip.gate.set_limit(original)
            reclip.fetch_gate.set_limit(original_fetch)


if __name__ == "__main__":
    unittest.main()
