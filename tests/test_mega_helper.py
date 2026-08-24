import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path

from mega_helper import MegaHelper, MegaHelperError


FAKE_RCLONE = r'''#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
command = args[0] if args else ""
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
        )
        self.helper.safety_bytes = 0

    def tearDown(self):
        self.helper._pool.shutdown(wait=True, cancel_futures=True)
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
            if jobs and all(job["status"] not in {"queued", "uploading"} for job in jobs):
                return jobs
            time.sleep(0.02)
        self.fail("uploads did not finish")

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
        grouped = next(job for job in jobs if job["filename"] == "large.mp4")
        self.assertEqual(grouped["remote_path"], "ReClip/Group A/large.mp4")

    def test_enqueue_rejects_selection_that_exceeds_every_account(self):
        self.helper.add_account("Only", "only@example.com", "secret")
        selection = [self.add_file("too-large.mp4", 11 * 1024 * 1024)]
        with self.assertRaises(MegaHelperError) as raised:
            self.helper.enqueue(selection)
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(self.helper.public_jobs(), [])


if __name__ == "__main__":
    unittest.main()
