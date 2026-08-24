import os
import tempfile
import threading
import time
import unittest
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
        with reclip.index_lock:
            reclip.download_index.clear()
        with reclip.groups_lock:
            reclip.groups.clear()
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

    def test_overhaul_is_index_and_legacy_ui_remains_available(self):
        root = self.client.get("/")
        self.assertEqual(root.status_code, 200)
        self.assertIn(b"<title>ReClip</title>", root.data)
        self.assertIn(b'id="megaOverlay"', root.data)
        self.assertIn(b'id="splitFetch"', root.data)

        legacy = self.client.get("/legacy")
        self.assertEqual(legacy.status_code, 200)
        self.assertIn(b"Free Media Downloader", legacy.data)

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

    def test_config_endpoint_updates_concurrency(self):
        original = reclip.gate.limit
        try:
            response = self.client.post("/api/config", json={"max_concurrent": 4})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["max_concurrent"], 4)
            self.assertEqual(self.client.get("/api/config").get_json()["max_concurrent"], 4)
        finally:
            reclip.gate.set_limit(original)


if __name__ == "__main__":
    unittest.main()
