import email.utils
import hashlib
import json
import unittest
from urllib.parse import parse_qs, urlparse

import gofile_helper
from gofile_helper import GoFileClient, GoFileError, MiniRacerSigner


SERVER_NOW = 1_793_575_277.0
UA = "UA-TEST"


def _file(file_id, name, code=None, **extra):
    data = {
        "id": file_id, "type": "file", "name": name, "code": code or f"c{file_id}",
        "size": 1000, "md5": "0" * 32, "mimetype": "video/mp4", "canAccess": True,
        "link": f"https://file-eu-par-1.gofile.io/download/web/{file_id}/{name}",
        "thumbnail": f"https://store8.gofile.io/download/web/{file_id}/thumb",
    }
    data.update(extra)
    return data


def _folder(folder_id, code, children, **extra):
    data = {"id": folder_id, "type": "folder", "code": code, "canAccess": True,
            "name": f"folder {code}", "children": {c["id"]: c for c in children}}
    data.update(extra)
    return data


class FakeSigner:
    def __init__(self, script, user_agent, language):
        self.script = script
        self.user_agent = user_agent
        self.language = language
        self.calls = []
        self.unexpected = []
        self.closed = False

    def sign(self, token, now_ms):
        self.calls.append((token, now_ms))
        digest = hashlib.sha256(f"{self.script}:{token}:{int(now_ms)}".encode()).hexdigest()
        return digest, list(self.unexpected)

    def close(self):
        self.closed = True


class FakeGoFile:
    """In-memory gofile.io: accounts, the signer script, and /contents."""

    def __init__(self, contents=None):
        self.contents = dict(contents or {})
        self.requests = []
        self.tokens = []
        self.script = "script-v1"
        self.page_size = None
        self.overrides = {}  # code -> (status, payload) for the next request

    def __call__(self, method, url, headers, data=None, timeout=20):
        self.requests.append((method, url, dict(headers)))
        parsed = urlparse(url)
        date = email.utils.formatdate(SERVER_NOW, usegmt=True)
        if parsed.netloc == "gofile.io" and parsed.path == gofile_helper.SCRIPT_PATH:
            return 200, {}, self.script.encode()
        if parsed.path == "/accounts" and method == "POST":
            token = f"token{len(self.tokens) + 1}"
            self.tokens.append(token)
            body = {"status": "ok", "data": {"token": token}}
            return 200, {"date": date}, json.dumps(body).encode()
        if parsed.path.startswith("/contents/"):
            key = parsed.path.rsplit("/", 1)[1]
            if key in self.overrides:
                status, payload = self.overrides.pop(key)
                return status, {"date": date}, json.dumps(payload).encode()
            data = self.contents.get(key)
            if data is None:
                return 404, {"date": date}, json.dumps({"status": "error-notFound"}).encode()
            query = parse_qs(parsed.query)
            page = int(query["page"][0])
            size = self.page_size or int(query["pageSize"][0])
            payload = {"status": "ok", "data": dict(data), "metadata": {}}
            if data.get("type") == "folder":
                children = list((data.get("children") or {}).items())
                chunk = children[(page - 1) * size: page * size]
                payload["data"]["children"] = dict(chunk)
                payload["metadata"] = {"hasNextPage": page * size < len(children)}
            return 200, {"date": date}, json.dumps(payload).encode()
        return 404, {}, b""

    def calls_to(self, path_prefix):
        return [r for r in self.requests if urlparse(r[1]).path.startswith(path_prefix)]


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def make_client(fake, clock=None, **kwargs):
    signers = []

    def factory(script, user_agent, language):
        signer = FakeSigner(script, user_agent, language)
        signers.append(signer)
        return signer

    client = GoFileClient(UA, transport=fake, signer_factory=factory,
                          clock=clock or Clock(SERVER_NOW), sleep=lambda _s: None,
                          **kwargs)
    return client, signers


class UrlTests(unittest.TestCase):
    def test_parses_share_urls(self):
        self.assertEqual(gofile_helper.gofile_code("https://gofile.io/d/ffSkcuQc"), "ffSkcuQc")
        self.assertEqual(gofile_helper.gofile_code("http://www.gofile.io/d/ab-12/?x=1"), "ab-12")
        self.assertIsNone(gofile_helper.gofile_code("https://gofile.io/"))
        self.assertIsNone(gofile_helper.gofile_code("https://evil.test/gofile.io/d/abc"))
        self.assertIsNone(gofile_helper.gofile_code("https://gofile.io.evil.test/d/abc"))


class ExpandTests(unittest.TestCase):
    def test_expands_folder_and_subfolders_with_one_listing_each(self):
        sub = _folder("s1", "SUB", [_file("f3", "3.mp4")])
        root = _folder("r1", "ROOT", [
            _file("f1", "1.mp4", code="AAA"),
            {"id": "s1", "type": "folder", "code": "SUB", "name": "sub"},
            _file("f2", "2.jpg", code="BBB", mimetype="image/jpeg"),
        ])
        fake = FakeGoFile({"ROOT": root, "SUB": sub})
        client, _signers = make_client(fake)

        leaves = client.expand("https://gofile.io/d/ROOT")

        self.assertEqual(leaves, ["https://gofile.io/d/AAA", "https://gofile.io/d/BBB",
                                  "https://gofile.io/d/cf3"])
        self.assertEqual(len(fake.calls_to("/contents/")), 2)
        # Cards and downloads come from the cached listings: no more API calls.
        info = client.card_info("https://gofile.io/d/AAA")
        target = client.resolve_download("https://gofile.io/d/cf3")
        self.assertEqual(info["title"], "1.mp4")
        self.assertEqual(target["filename"], "3.mp4")
        self.assertEqual(target["cookie"], "accountToken=token1")
        self.assertEqual(target["size"], 1000)
        self.assertEqual(len(fake.calls_to("/contents/")), 2)

    def test_listing_follows_pagination(self):
        root = _folder("r1", "ROOT", [_file(f"f{i}", f"{i}.mp4") for i in range(5)])
        fake = FakeGoFile({"ROOT": root})
        fake.page_size = 2
        client, _signers = make_client(fake)

        self.assertEqual(len(client.expand("https://gofile.io/d/ROOT")), 5)
        self.assertEqual(len(fake.calls_to("/contents/")), 3)

    def test_single_file_link_expands_to_itself(self):
        fake = FakeGoFile({"FILE": _file("f1", "clip.mp4", code="FILE")})
        client, _signers = make_client(fake)
        self.assertEqual(client.expand("https://gofile.io/d/FILE"), ["https://gofile.io/d/FILE"])

    def test_locked_subfolder_stays_visible_as_its_own_url(self):
        root = _folder("r1", "ROOT", [{"id": "s1", "type": "folder", "code": "LOCK"}])
        locked = {"id": "s1", "type": "folder", "code": "LOCK", "canAccess": False,
                  "password": True}
        fake = FakeGoFile({"ROOT": root, "LOCK": locked})
        client, _signers = make_client(fake)

        self.assertEqual(client.expand("https://gofile.io/d/ROOT"), ["https://gofile.io/d/LOCK"])
        with self.assertRaisesRegex(GoFileError, "password-protected"):
            client.card_info("https://gofile.io/d/LOCK")

    def test_max_files_caps_expansion(self):
        root = _folder("r1", "ROOT", [_file(f"f{i}", f"{i}.mp4") for i in range(10)])
        client, _signers = make_client(FakeGoFile({"ROOT": root}), max_files=4)
        self.assertEqual(len(client.expand("https://gofile.io/d/ROOT")), 4)


class BlockedFileTests(unittest.TestCase):
    def assert_blocked(self, flags, message):
        root = _folder("r1", "ROOT", [_file("f1", "1.mp4", code="AAA", **flags)])
        client, _signers = make_client(FakeGoFile({"ROOT": root}))
        client.expand("https://gofile.io/d/ROOT")
        with self.assertRaises(GoFileError) as card_error:
            client.card_info("https://gofile.io/d/AAA")
        with self.assertRaises(GoFileError) as download_error:
            client.resolve_download("https://gofile.io/d/AAA")
        self.assertEqual(str(card_error.exception), message)
        self.assertEqual(str(download_error.exception), message)
        self.assertLessEqual(len(message), 120, "UI cards truncate at 120 characters")

    def test_overloaded_file_is_reported_not_downloaded(self):
        self.assert_blocked({"overloaded": True}, gofile_helper.MSG_OVERLOADED)

    def test_frozen_file_is_reported(self):
        self.assert_blocked({"isFrozen": True}, gofile_helper.MSG_FROZEN)

    def test_file_without_link_is_reported(self):
        self.assert_blocked({"link": None}, gofile_helper.MSG_NO_LINK)

    def test_link_outside_gofile_is_rejected(self):
        self.assert_blocked({"link": "https://169.254.169.254/latest"}, gofile_helper.MSG_NO_LINK)

    def test_gates_and_missing_links(self):
        cases = {
            "PW": ({"canAccess": False, "password": True}, "password-protected"),
            "PRIV": ({"canAccess": False, "public": False}, "private"),
            "EXP": ({"canAccess": False, "expire": 1}, "expired"),
        }
        contents = {code: dict({"id": code, "type": "folder", "code": code}, **flags)
                    for code, (flags, _msg) in cases.items()}
        fake = FakeGoFile(contents)
        client, _signers = make_client(fake)
        for code, (_flags, message) in cases.items():
            with self.assertRaisesRegex(GoFileError, message):
                client.expand(f"https://gofile.io/d/{code}")
        with self.assertRaisesRegex(GoFileError, "not found"):
            client.card_info("https://gofile.io/d/GONE")
        before = len(fake.calls_to("/contents/"))
        with self.assertRaisesRegex(GoFileError, "not found"):
            client.card_info("https://gofile.io/d/GONE")
        self.assertEqual(len(fake.calls_to("/contents/")), before, "permanent errors are cached")

    def test_explains_file_server_failures(self):
        explain = GoFileClient.explain_download_error
        self.assertIn("HTTP 403", explain("curl: (22) The requested URL returned error: 403"))
        self.assertIn("no longer exists", explain("curl: (22) The requested URL returned error: 404"))
        self.assertIn("HTTP 429", explain("curl: (22) The requested URL returned error: 429"))
        self.assertIn("HTTP 502", explain("curl: (22) The requested URL returned error: 502"))
        self.assertIsNone(explain("curl: (6) Could not resolve host"))
        for code in (403, 404, 429, 503):
            self.assertLessEqual(len(explain(f"returned error: {code}")), 120)


class SigningTests(unittest.TestCase):
    def test_signed_request_headers_and_server_clock(self):
        root = _folder("r1", "ROOT", [_file("f1", "1.mp4")])
        fake = FakeGoFile({"ROOT": root})
        # Local clock is an hour ahead of GoFile: the signer must use GoFile's.
        client, signers = make_client(fake, clock=Clock(SERVER_NOW + 3600))
        client.expand("https://gofile.io/d/ROOT")

        method, _url, headers = fake.calls_to("/contents/")[0]
        token, now_ms = signers[0].calls[0]
        self.assertEqual(method, "GET")
        self.assertEqual(headers["Authorization"], "Bearer token1")
        self.assertEqual(headers["X-BL"], "en-US")
        self.assertEqual(headers["User-Agent"], UA)
        self.assertEqual(token, "token1")
        self.assertEqual(headers["X-Website-Token"], signers[0].sign(token, now_ms)[0])
        expected_ms = (SERVER_NOW - gofile_helper.CLOCK_SAFETY_SECONDS) * 1000
        self.assertAlmostEqual(now_ms, expected_ms, delta=1000)

    def test_unexpected_signer_inputs_block_the_request(self):
        fake = FakeGoFile({"ROOT": _folder("r1", "ROOT", [])})

        def factory(script, user_agent, language):
            signer = FakeSigner(script, user_agent, language)
            signer.unexpected = ["navigator.platform"]
            return signer

        client = GoFileClient(UA, transport=fake, signer_factory=factory,
                              clock=Clock(SERVER_NOW), sleep=lambda _s: None)
        with self.assertRaisesRegex(GoFileError, "avoid an IP ban"):
            client.expand("https://gofile.io/d/ROOT")
        self.assertEqual(fake.calls_to("/contents/"), [], "never send an unverified signature")

    def test_broken_signer_output_blocks_the_request(self):
        fake = FakeGoFile({"ROOT": _folder("r1", "ROOT", [])})

        class BadSigner(FakeSigner):
            def sign(self, token, now_ms):
                return "[object Promise]", []

        client = GoFileClient(UA, transport=fake, signer_factory=BadSigner,
                              clock=Clock(SERVER_NOW), sleep=lambda _s: None)
        with self.assertRaises(GoFileError):
            client.expand("https://gofile.io/d/ROOT")
        self.assertEqual(fake.calls_to("/contents/"), [])

    def test_script_is_refetched_after_ttl(self):
        root = _folder("r1", "ROOT", [_file("f1", "1.mp4")])
        fake = FakeGoFile({"ROOT": root})
        clock = Clock(SERVER_NOW)
        client, signers = make_client(fake, clock=clock, script_ttl=100, listing_ttl=10)
        client.expand("https://gofile.io/d/ROOT")
        clock.now += 50
        client.expand("https://gofile.io/d/ROOT")
        self.assertEqual(len(signers), 1)
        fake.script = "script-v2"
        clock.now += 60
        client.expand("https://gofile.io/d/ROOT")
        self.assertEqual(len(signers), 2)
        self.assertEqual(signers[1].script, "script-v2")
        self.assertTrue(signers[0].closed)

    def test_rejected_token_renews_the_guest_session_once(self):
        root = _folder("r1", "ROOT", [_file("f1", "1.mp4")])
        fake = FakeGoFile({"ROOT": root})
        fake.overrides["ROOT"] = (401, {"status": "error-token"})
        client, _signers = make_client(fake)

        client.expand("https://gofile.io/d/ROOT")
        target = client.resolve_download("https://gofile.io/d/cf1")
        self.assertEqual(fake.tokens, ["token1", "token2"])
        self.assertEqual(target["cookie"], "accountToken=token2")

    def test_rate_limit_pauses_all_gofile_requests(self):
        fake = FakeGoFile({"ROOT": _folder("r1", "ROOT", [])})
        fake.overrides["ROOT"] = (429, {"status": "error-rateLimit"})
        clock = Clock(SERVER_NOW)
        client, _signers = make_client(fake, clock=clock)

        with self.assertRaisesRegex(GoFileError, "rate-limiting"):
            client.expand("https://gofile.io/d/ROOT")
        sent = len(fake.requests)
        with self.assertRaisesRegex(GoFileError, r"Retry in ~5 min"):
            client.expand("https://gofile.io/d/ROOT")
        self.assertEqual(len(fake.requests), sent, "cooldown must not touch the network")
        clock.now += gofile_helper.RATE_LIMIT_COOLDOWN + 1
        self.assertEqual(client.expand("https://gofile.io/d/ROOT"), [])

    def test_bare_refusal_backs_off_and_drops_the_signer(self):
        fake = FakeGoFile({"ROOT": _folder("r1", "ROOT", [])})
        fake.overrides["ROOT"] = (403, {"status": "error-forbidden"})
        clock = Clock(SERVER_NOW)
        client, signers = make_client(fake, clock=clock)

        with self.assertRaisesRegex(GoFileError, "avoid an IP ban"):
            client.expand("https://gofile.io/d/ROOT")
        self.assertTrue(signers[0].closed)
        clock.now += gofile_helper.REFUSED_COOLDOWN + 1
        client.expand("https://gofile.io/d/ROOT")
        self.assertEqual(len(signers), 2, "a fresh script is loaded after a refusal")


class SandboxTests(unittest.TestCase):
    """Runs a stand-in for wt.obf.js through the real V8 sandbox."""

    SCRIPT = (
        "function hex(s){var h='';for(var i=0;i<s.length;i++)"
        "h+=('0'+s.charCodeAt(i).toString(16)).slice(-2);return h;}"
        "window.generateWT=function(t){var w=Math.floor(Date.now()/1000/14400);"
        "return (hex(navigator.userAgent+'|'+navigator.language+'|'+t+'|'+w)"
        "+'0'.repeat(64)).slice(0,64);};"
    )

    def setUp(self):
        try:
            import py_mini_racer  # noqa: F401
        except ImportError:
            self.skipTest("mini-racer not installed")

    def test_sandbox_pins_clock_and_exposes_only_ua_and_language(self):
        signer = MiniRacerSigner(self.SCRIPT, "UA", "en-US")
        try:
            wt, unexpected = signer.sign("TK", 14400 * 1000 * 7 + 5)
        finally:
            signer.close()
        expected = "UA|en-US|TK|7".encode().hex()
        self.assertEqual(unexpected, [])
        self.assertEqual(wt, (expected + "0" * 64)[:64])

    def test_sandbox_reports_extra_fingerprint_reads(self):
        script = self.SCRIPT.replace("navigator.language", "navigator.platform")
        signer = MiniRacerSigner(script, "UA", "en-US")
        try:
            _wt, unexpected = signer.sign("TK", 0)
        finally:
            signer.close()
        self.assertEqual(unexpected, ["navigator.platform"])

    def test_script_without_generator_is_rejected(self):
        with self.assertRaisesRegex(GoFileError, "avoid an IP ban"):
            MiniRacerSigner("var nothing = 1;", "UA", "en-US")


if __name__ == "__main__":
    unittest.main()
