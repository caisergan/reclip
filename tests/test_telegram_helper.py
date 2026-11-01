import asyncio
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

import telegram_helper as th

HAS_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


class FakeMessage:
    def __init__(self, message_id):
        self.id = message_id


class FakeClient:
    def __init__(self):
        self.sent = []

    async def send_file(self, entity, file, **kwargs):
        self.sent.append({"name": os.path.basename(file), "size": os.path.getsize(file), **kwargs})
        return FakeMessage(len(self.sent))


def fake_channel(username=None):
    from telethon.tl.types import Channel, ChatPhotoEmpty

    return Channel(id=777, title="Archive", photo=ChatPhotoEmpty(), date=None, username=username,
                   broadcast=True, creator=True)


class StateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        patcher = mock.patch.dict(os.environ, {"RECLIP_TELEGRAM_DIR": os.path.join(self.tmp, "tg")})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_config_round_trip_is_private(self):
        th.save_config({"api_id": 1, "api_hash": "x", "chat": -100123})
        self.assertEqual(th.load_config()["chat"], -100123)
        self.assertEqual(stat.S_IMODE(os.stat(th._config_path()).st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(os.stat(th.state_dir()).st_mode), 0o700)

    def test_credentials_required_when_not_interactive(self):
        with mock.patch.dict(os.environ, {"TG_API_ID": "", "TG_API_HASH": ""}):
            with self.assertRaises(th.TelegramHelperError):
                th.credentials({}, interactive=False)

    def test_env_credentials_win(self):
        with mock.patch.dict(os.environ, {"TG_API_ID": "42", "TG_API_HASH": "abc"}):
            self.assertEqual(th.credentials({}, interactive=False), (42, "abc"))


class PureHelperTests(unittest.TestCase):
    def test_parse_chat(self):
        self.assertEqual(th.parse_chat("-1001234"), -1001234)
        self.assertEqual(th.parse_chat(" me "), "me")
        self.assertEqual(th.parse_chat("@archive"), "@archive")

    def test_plan_parts_leaves_headroom(self):
        self.assertEqual(th.plan_parts(100, 1000), 1)
        self.assertEqual(th.plan_parts(th.FREE_LIMIT, th.FREE_LIMIT), 2)
        self.assertEqual(th.plan_parts(5 * th.FREE_LIMIT, th.FREE_LIMIT), 6)

    def test_message_links(self):
        self.assertEqual(th.message_link(fake_channel(), 5), "https://t.me/c/777/5")
        self.assertEqual(th.message_link(fake_channel("pub"), 5), "https://t.me/pub/5")
        self.assertIsNone(th.message_link(object(), 5))

    def test_unsplittable_oversized_file_errors(self):
        with self.assertRaises(th.TelegramHelperError):
            th.split_video("/x/song.mp3", None, 10, "/tmp")


@unittest.skipUnless(HAS_FFMPEG, "ffmpeg/ffprobe not installed")
class UploadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.video = os.path.join(cls.tmp, "clip.mp4")
        subprocess.run(
            [
                "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=320x240:rate=25",
                "-f", "lavfi", "-i", "sine=frequency=440", "-t", "12",
                "-c:v", "libx264", "-g", "25", "-c:a", "aac", "-shortest", cls.video,
            ],
            check=True,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, True)

    def test_small_file_uploads_once_with_video_metadata(self):
        client = FakeClient()
        result = asyncio.run(th.upload_file(client, fake_channel(), self.video, th.FREE_LIMIT))
        self.assertEqual(result["parts"], 1)
        self.assertEqual(result["links"], ["https://t.me/c/777/1"])
        sent = client.sent[0]
        self.assertEqual(sent["caption"], "clip.mp4")
        attr = sent["attributes"][0]
        self.assertEqual((attr.w, attr.h), (320, 240))
        self.assertAlmostEqual(attr.duration, 12, delta=0.5)
        self.assertTrue(sent["supports_streaming"])

    def test_oversized_file_is_split_under_limit_and_cleaned_up(self):
        client = FakeClient()
        limit = os.path.getsize(self.video) // 2
        seen = []
        result = asyncio.run(
            th.upload_file(client, fake_channel(), self.video, limit, lambda *a: seen.append(a))
        )
        self.assertGreaterEqual(result["parts"], 3)
        self.assertEqual(len(client.sent), result["parts"])
        for i, sent in enumerate(client.sent, 1):
            self.assertLessEqual(sent["size"], limit)
            self.assertEqual(sent["caption"], f"clip.mp4 (part {i}/{result['parts']})")
            self.assertTrue(sent["name"].startswith("clip (part "))
        self.assertFalse([d for d in os.listdir(self.tmp) if d.startswith(".reclip-tg-")])
        self.assertTrue(os.path.exists(self.video))


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# TelegramService (web suite) against a fake Telethon client

import json  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402
from types import SimpleNamespace  # noqa: E402


class FakeTelethon:
    """Async stand-in for TelegramClient covering what TelegramService calls."""

    def __init__(self, *, authorized=True, premium=False, needs_password=False):
        self.connected = False
        self.authorized = authorized
        self.premium = premium
        self.needs_password = needs_password
        self.uploads = []
        self.sends = []
        self.block = None  # threading.Event: upload_file waits on it when set
        self.fail_send = None
        self.next_id = 100

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        return SimpleNamespace(first_name="Ege", last_name="", username="ege", premium=self.premium)

    async def get_entity(self, chat):
        if chat == "missing":
            raise ValueError("no such chat")
        return fake_channel()

    async def get_dialogs(self):
        return []

    async def iter_dialogs(self, limit=None):
        from telethon.tl.types import Channel, ChatAdminRights, ChatPhotoEmpty

        yield SimpleNamespace(id=-100777, name="Archive", entity=fake_channel())
        rights = ChatAdminRights(post_messages=False)
        yield SimpleNamespace(id=-100888, name="Not mine", entity=Channel(
            id=888, title="Not mine", photo=ChatPhotoEmpty(), date=None, broadcast=True, admin_rights=rights))
        yield SimpleNamespace(id=42, name="A person", entity=SimpleNamespace())

    async def send_code_request(self, phone):
        from telethon.tl.types.auth import SentCodeTypeApp

        return SimpleNamespace(phone_code_hash="hash", type=SentCodeTypeApp(length=5))

    async def sign_in(self, phone=None, code=None, *, password=None, phone_code_hash=None):
        from telethon import errors

        if code is not None:
            if code != "12345":
                raise errors.PhoneCodeInvalidError(request=None)
            if self.needs_password:
                raise errors.SessionPasswordNeededError(request=None)
        elif password != "secret":
            raise errors.PasswordHashInvalidError(request=None)
        self.authorized = True

    async def log_out(self):
        self.authorized = False

    async def upload_file(self, path, progress_callback=None):
        import asyncio as _asyncio
        from telethon.tl.types import InputFile

        if self.block is not None:
            while not self.block.is_set():
                await _asyncio.sleep(0.01)
        size = os.path.getsize(path)
        if progress_callback:
            progress_callback(size // 2, size)
            progress_callback(size, size)
        self.uploads.append(os.path.basename(path))
        return InputFile(id=len(self.uploads), parts=1, name=os.path.basename(path), md5_checksum="")

    async def send_file(self, entity, file, caption=None, **kwargs):
        if self.fail_send:
            raise self.fail_send
        many = isinstance(file, list)
        media = file if many else [file]
        self.sends.append({"count": len(media), "album": many, "caption": caption, "media": media, **kwargs})
        messages = []
        for _ in media:
            self.next_id += 1
            messages.append(FakeMessage(self.next_id))
        return messages if many else messages[0]


@unittest.skipUnless(HAS_FFMPEG, "ffmpeg/ffprobe not installed")
class TelegramServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.media = tempfile.mkdtemp()
        source = os.path.join(cls.media, "clip0.mp4")
        subprocess.run(
            ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=320x240:rate=25",
             "-t", "3", "-c:v", "libx264", "-g", "25", source],
            check=True,
        )
        for i in range(1, 12):
            shutil.copy(source, os.path.join(cls.media, f"clip{i}.mp4"))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.media, True)

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        env = mock.patch.dict(os.environ, {"RECLIP_TELEGRAM_DIR": os.path.join(self.tmp, "tg"),
                                           "TG_API_ID": "", "TG_API_HASH": ""})
        env.start()
        self.addCleanup(env.stop)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        th.save_config({"api_id": 1, "api_hash": "a" * 32})
        self.fake = FakeTelethon()
        self.posted = []
        self.service = self.make_service()

    def make_service(self):
        return th.TelegramService(
            self.resolve, on_posted=self.posted.append,
            client_factory=lambda *_args: self.fake,
        )

    def resolve(self, selection):
        out = []
        for sel in selection:
            path = os.path.join(self.media, sel["filename"])
            if not os.path.exists(path):
                raise th.TelegramHelperError(f"Library item not found: {sel['filename']}", 404)
            out.append({"path": path, "filename": sel["filename"], "group_id": sel.get("group_id", ""),
                        "size": os.path.getsize(path)})
        return out

    def files(self, n):
        return [{"filename": f"clip{i}.mp4", "group_id": "g"} for i in range(n)]

    def wait_final(self, post_id, timeout=20):
        deadline = time.time() + timeout
        while time.time() < deadline:
            post = next(p for p in self.service.list_posts() if p["id"] == post_id)
            if post["status"] in th.POST_FINAL:
                return post
            time.sleep(0.02)
        self.fail(f"post {post_id} did not finish")

    def test_album_with_caption_on_first_video(self):
        post = self.service.create_post({"files": self.files(3), "chat": "-100777",
                                         "caption": "**Weekend** dump", "parse_mode": "markdown"})
        self.assertEqual(post["albums"], [3])
        self.assertNotIn("path", post["items"][0])
        done = self.wait_final(post["id"])
        self.assertEqual(done["status"], "done", done.get("error"))
        self.assertEqual(len(self.fake.sends), 1)
        send = self.fake.sends[0]
        self.assertTrue(send["album"])
        self.assertEqual(send["caption"], ["**Weekend** dump"])
        self.assertEqual(send["parse_mode"], "md")
        self.assertTrue(send["supports_streaming"])
        video_attr = send["media"][0].attributes[0]
        self.assertEqual((video_attr.w, video_attr.h), (320, 240))
        self.assertIsNotNone(send["media"][0].thumb)
        self.assertEqual([i["status"] for i in done["items"]], ["sent"] * 3)
        self.assertEqual(done["uploaded_bytes"], done["total_bytes"])
        self.assertEqual(done["links"], ["https://t.me/c/777/101"])
        self.assertEqual(len(self.posted), 1)
        self.assertEqual(self.posted[0]["chat_title"], "Archive")

    def test_more_than_ten_videos_become_balanced_albums_with_one_caption(self):
        post = self.service.create_post({"files": self.files(12), "chat": "me", "caption": "hello"})
        self.assertEqual(post["albums"], [6, 6])
        done = self.wait_final(post["id"])
        self.assertEqual(done["status"], "done", done.get("error"))
        self.assertEqual([s["count"] for s in self.fake.sends], [6, 6])
        self.assertEqual([s["caption"] for s in self.fake.sends], [["hello"], [""]])
        self.assertIsNone(self.fake.sends[0]["parse_mode"])
        self.assertEqual(len(done["message_ids"]), 12)

    def test_single_video_is_sent_as_one_message(self):
        done = self.wait_final(self.service.create_post(
            {"files": self.files(1), "chat": "me", "caption": "solo", "silent": True})["id"])
        self.assertEqual(done["status"], "done", done.get("error"))
        send = self.fake.sends[0]
        self.assertFalse(send["album"])
        self.assertEqual(send["caption"], "solo")
        self.assertTrue(send["silent"])

    def test_validation(self):
        cases = [
            ({"files": [], "chat": "me"}, "at least one"),
            ({"files": self.files(1), "chat": "me", "parse_mode": "bbcode"}, "format"),
            ({"files": self.files(1), "chat": "me", "caption": "x" * 1025}, "1024"),
            ({"files": [{"filename": "nope.mp4", "group_id": ""}], "chat": "me"}, "not found"),
            ({"files": self.files(1), "chat": "missing"}, "find that chat"),
        ]
        for payload, message in cases:
            with self.subTest(message=message):
                with self.assertRaises(th.TelegramHelperError) as ctx:
                    self.service.create_post(payload)
                self.assertIn(message, ctx.exception.message)
        # Markdown markup doesn't count toward the limit; Premium raises it.
        self.service.create_post({"files": self.files(1), "chat": "me", "caption": "**" + "x" * 1024 + "**",
                                  "parse_mode": "markdown"})
        self.fake.premium = True
        self.service._me = None
        self.service.create_post({"files": self.files(1), "chat": "me", "caption": "x" * 3000})

    def test_cancel_running_post_and_retry(self):
        self.fake.block = threading.Event()
        post = self.service.create_post({"files": self.files(2), "chat": "me", "caption": "c"})
        deadline = time.time() + 10
        while not self.service.has_active_upload("clip0.mp4", "g") or \
                next(p for p in self.service.list_posts() if p["id"] == post["id"])["status"] != "uploading":
            self.assertLess(time.time(), deadline)
            time.sleep(0.01)
        self.service.cancel_post(post["id"])
        cancelled = self.wait_final(post["id"])
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertEqual(self.fake.sends, [])
        self.assertFalse(self.service.has_active_upload("clip0.mp4", "g"))

        self.fake.block = None
        retried = self.service.retry_post(post["id"])
        self.assertNotEqual(retried["id"], post["id"])
        self.assertEqual(self.wait_final(retried["id"])["status"], "done")
        self.assertEqual(self.service.clear_finished(), 2)
        self.assertEqual(self.service.list_posts(), [])
        leftovers = [d for d in os.listdir(self.service.dir) if d.startswith("work-")]
        self.assertEqual(leftovers, [])

    def test_send_failure_is_reported(self):
        from telethon import errors

        self.fake.fail_send = errors.FloodWaitError(request=None, capture=600)
        failed = self.wait_final(self.service.create_post({"files": self.files(2), "chat": "me"})["id"])
        self.assertEqual(failed["status"], "error")
        self.assertIn("10 min", failed["error"])
        self.assertEqual(self.posted, [])

    def test_interrupted_posts_become_retryable_after_restart(self):
        with open(self.service.posts_path, "w") as fh:
            json.dump([{"id": "abc", "status": "uploading", "items": [], "created_at": 1}], fh)
        restarted = self.make_service()
        post = restarted.list_posts()[0]
        self.assertEqual(post["status"], "error")
        self.assertIn("restarted", post["error"])

    def test_queued_posts_resume_on_startup_without_opening_the_ui(self):
        items = [{**f, "uploaded": 0, "parts": 1, "status": "queued"} for f in self.resolve(self.files(2))]
        queued = {"id": "waiting1", "status": "queued", "chat": "me", "chat_title": "Saved Messages",
                  "caption": "after restart", "parse_mode": "plain", "silent": False, "items": items,
                  "albums": [2], "total_bytes": sum(i["size"] for i in items), "uploaded_bytes": 0,
                  "speed": None, "eta": None, "current": None, "message_ids": [], "links": [],
                  "error": None, "created_at": time.time(), "started_at": None, "finished_at": None}
        os.makedirs(os.path.dirname(self.service.posts_path), exist_ok=True)
        with open(self.service.posts_path, "w") as fh:
            json.dump([queued], fh)
        self.service = self.make_service()  # nothing calls status() or opens the UI
        done = self.wait_final("waiting1")
        self.assertEqual(done["status"], "done", done.get("error"))
        self.assertEqual(self.fake.sends[0]["caption"], ["after restart"])

    def test_credentials_cannot_change_mid_upload(self):
        self.fake.block = threading.Event()
        self.addCleanup(self.fake.block.set)
        self.service.create_post({"files": self.files(1), "chat": "me"})
        with self.assertRaises(th.TelegramHelperError) as ctx:
            self.service.set_credentials("12345", "b" * 32)
        self.assertEqual(ctx.exception.status, 409)

    def test_browser_login_flow_with_two_step_password(self):
        self.fake.authorized = False
        self.fake.needs_password = True
        self.assertFalse(self.service.status()["authorized"])
        with self.assertRaises(th.TelegramHelperError):
            self.service.set_credentials("12a", "x")
        self.service.set_credentials("12345", "b" * 32)
        self.assertEqual(th.load_config()["api_id"], 12345)
        with self.assertRaises(th.TelegramHelperError):
            self.service.send_code("call me")
        self.assertEqual(self.service.send_code("+90 555 123 45 67"), {"sent_to": "Telegram app"})
        self.assertEqual(self.service.status()["login_step"], "code")
        with self.assertRaises(th.TelegramHelperError):
            self.service.verify_code("11111")
        self.assertEqual(self.service.verify_code("12345"), {"needs_password": True})
        with self.assertRaises(th.TelegramHelperError):
            self.service.verify_password("wrong")
        self.service.verify_password("secret")
        status = self.service.status()
        self.assertTrue(status["authorized"])
        self.assertIsNone(status["login_step"])
        self.assertEqual(status["me"]["username"], "ege")
        self.service.logout()
        self.assertFalse(self.service.status()["authorized"])

    def test_chat_list_only_includes_postable_chats(self):
        chats = self.service.list_chats()
        self.assertEqual([(c["id"], c["kind"]) for c in chats], [("me", "saved"), ("-100777", "channel")])
        self.assertEqual(self.service.set_default_chat("-100777"), {"chat": "-100777", "title": "Archive"})
        self.assertEqual(self.service.status()["default_chat"], "-100777")

    def test_routes(self):
        from flask import Flask

        app = Flask(__name__)
        app.register_blueprint(self.service.blueprint)
        client = app.test_client()
        self.assertTrue(client.get("/api/telegram/status").get_json()["authorized"])
        bad = client.post("/api/telegram/posts", json={"files": [], "chat": "me"})
        self.assertEqual(bad.status_code, 400)
        created = client.post("/api/telegram/posts", json={"files": self.files(2), "chat": "me"})
        self.assertEqual(created.status_code, 201)
        self.wait_final(created.get_json()["post"]["id"])
        self.assertEqual(len(client.get("/api/telegram/posts").get_json()["posts"]), 1)
        self.assertEqual(client.post("/api/telegram/posts/nope/cancel").status_code, 404)
