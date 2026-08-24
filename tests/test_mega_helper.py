import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path

from mega_helper import MegaHelper, MegaHelperError, is_mega_public_url


FAKE_RCLONE = r'''#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
command = args[0] if args else ""
log_path = os.environ.get("FAKE_RCLONE_LOG")
if log_path:
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(command + "\n")
if command == "version":
    print("rclone v-test")
    raise SystemExit(0)
if command == "help" and len(args) > 1 and args[1] == "backends":
    print("  mega         Mega")
    raise SystemExit(0)
if command == "obscure":
    sys.stdin.readline()
    print("obscured-test-value")
    raise SystemExit(0)
if command == "about":
    total = 10 * 1024 * 1024
    print(json.dumps({"total": total, "used": 0, "free": total}))
    raise SystemExit(0)
if command == "copyto":
    source = args[1]
    size = os.path.getsize(source)
    print(json.dumps({
        "level": "info",
        "msg": "stats",
        "stats": {"bytes": size, "totalBytes": size, "speed": 1024, "eta": 0},
    }), flush=True)
    raise SystemExit(0)
if command == "link":
    destination = args[1]
    if "link-fail" in destination and os.environ.get("FAKE_RCLONE_LINK_OK") != "1":
        print("public links temporarily unavailable", file=sys.stderr)
        raise SystemExit(1)
    # Current rclone/MEGA versions can return the legacy official host.
    print("https://mega.co.nz/file/test-node#test-key")
    raise SystemExit(0)
print("unsupported fake command", file=sys.stderr)
raise SystemExit(2)
'''


class MegaHelperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.downloads = root / "downloads"
        self.downloads.mkdir()
        self.fake = root / "rclone"
        self.fake.write_text(FAKE_RCLONE)
        self.fake.chmod(self.fake.stat().st_mode | stat.S_IXUSR)
        self.files = {}
        self.linked = []
        self.command_log = root / "rclone.log"
        os.environ["FAKE_RCLONE_LOG"] = str(self.command_log)
        os.environ.pop("FAKE_RCLONE_LINK_OK", None)

        def resolve(selection):
            result = []
            for item in selection:
                key = (item.get("group_id", ""), item["filename"])
                if key not in self.files:
                    raise ValueError("missing")
                result.append({
                    "path": str(self.files[key]),
                    "group_id": key[0],
                    "group_name": "Group A" if key[0] else "",
                })
            return result

        self.helper = MegaHelper(
            str(self.downloads),
            resolve,
            state_dir=str(root / "state"),
            rclone_bin=str(self.fake),
            max_workers=2,
            on_link_ready=self.linked.append,
        )
        self.helper.safety_bytes = 0

    def tearDown(self):
        self.helper._pool.shutdown(wait=True, cancel_futures=True)
        os.environ.pop("FAKE_RCLONE_LOG", None)
        os.environ.pop("FAKE_RCLONE_LINK_OK", None)
        self.temp.cleanup()

    def add_file(self, name, size, group_id=""):
        folder = self.downloads / (group_id or "root")
        folder.mkdir(exist_ok=True)
        path = folder / name
        with path.open("wb") as handle:
            handle.truncate(size)
        self.files[(group_id, name)] = path
        return {"filename": name, "group_id": group_id}

    def wait_for_uploads(self):
        deadline = time.time() + 5
        while time.time() < deadline:
            jobs = self.helper.public_jobs()
            if jobs and all(job["status"] not in {"queued", "uploading", "linking"} for job in jobs):
                return jobs
            time.sleep(0.02)
        self.fail("uploads did not finish")

    def test_public_link_validation_accepts_both_official_mega_hosts(self):
        self.assertTrue(is_mega_public_url("https://mega.nz/file/node#key"))
        self.assertTrue(is_mega_public_url("https://mega.co.nz/#!node!key"))
        self.assertFalse(is_mega_public_url("http://mega.co.nz/file/node#key"))
        self.assertFalse(is_mega_public_url("https://mega.co.nz.example.com/file/node#key"))

    def test_account_password_is_not_stored_in_clear_text(self):
        account = self.helper.add_account("Primary", "one@example.com", "super-secret")
        self.assertEqual(account["quota"]["free"], 10 * 1024 * 1024)
        config = Path(self.helper.rclone_config).read_text()
        self.assertNotIn("super-secret", config)
        self.assertIn("obscured-test-value", config)
        self.assertEqual(stat.S_IMODE(os.stat(self.helper.rclone_config).st_mode), 0o600)

    def test_auto_allocation_distributes_files_by_free_storage(self):
        first = self.helper.add_account("First", "first@example.com", "secret")
        second = self.helper.add_account("Second", "second@example.com", "secret")
        selection = [
            self.add_file("large.mp4", 6 * 1024 * 1024, "group-a"),
            self.add_file("small.mp4", 4 * 1024 * 1024),
        ]
        result = self.helper.enqueue(selection, folder="ReClip", preserve_groups=True)
        self.assertEqual(len(result["jobs"]), 2)
        self.assertEqual({job["account_id"] for job in result["jobs"]}, {first["id"], second["id"]})
        jobs = self.wait_for_uploads()
        self.assertTrue(all(job["status"] == "done" for job in jobs))
        self.assertTrue(all(job["public_url"].startswith("https://mega.co.nz/") for job in jobs))
        self.assertEqual(len(self.linked), 2)
        self.assertTrue(all(item["source_path"] for item in self.linked))
        grouped = next(job for job in jobs if job["filename"] == "large.mp4")
        self.assertEqual(grouped["remote_path"], "ReClip/Group A/large.mp4")

    def test_link_failure_can_be_retried_without_uploading_again(self):
        self.helper.add_account("Only", "only@example.com", "secret")
        selection = [self.add_file("link-fail.mp4", 1024)]
        result = self.helper.enqueue(selection)
        job_id = result["jobs"][0]["id"]
        jobs = self.wait_for_uploads()
        self.assertEqual(jobs[0]["status"], "error")
        self.assertTrue(jobs[0]["remote_uploaded"])
        self.assertIsNone(jobs[0]["public_url"])
        self.assertIn("Upload completed", jobs[0]["error"])

        os.environ["FAKE_RCLONE_LINK_OK"] = "1"
        retried = self.helper.create_public_link(job_id)
        self.assertEqual(retried["status"], "done")
        self.assertEqual(retried["public_url"], "https://mega.co.nz/file/test-node#test-key")
        commands = self.command_log.read_text().splitlines()
        self.assertEqual(commands.count("copyto"), 1)
        self.assertEqual(commands.count("link"), 2)

    def test_public_url_survives_helper_reload_and_reconciles(self):
        self.helper.add_account("Only", "only@example.com", "secret")
        self.helper.enqueue([self.add_file("persisted.mp4", 1024)])
        jobs = self.wait_for_uploads()
        self.assertEqual(jobs[0]["status"], "done")

        restored_links = []
        second = MegaHelper(
            str(self.downloads),
            self.helper.file_resolver,
            state_dir=self.helper.state_dir,
            rclone_bin=str(self.fake),
            max_workers=1,
            on_link_ready=restored_links.append,
        )
        try:
            restored = second.public_jobs()[0]
            self.assertEqual(restored["public_url"], jobs[0]["public_url"])
            self.assertEqual(second.reconcile_links(), 1)
            self.assertEqual(restored_links[0]["filename"], "persisted.mp4")
        finally:
            second._pool.shutdown(wait=True, cancel_futures=True)

    def test_enqueue_rejects_selection_that_exceeds_every_account(self):
        self.helper.add_account("Only", "only@example.com", "secret")
        selection = [self.add_file("too-large.mp4", 11 * 1024 * 1024)]
        with self.assertRaises(MegaHelperError) as raised:
            self.helper.enqueue(selection)
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(self.helper.public_jobs(), [])


if __name__ == "__main__":
    unittest.main()
