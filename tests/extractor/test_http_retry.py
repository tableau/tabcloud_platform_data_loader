"""Unit tests for extractor http_retry (backoff, 401, post timeout)."""

import unittest
from unittest.mock import MagicMock, patch

import requests

from extractor.http_retry import (
    is_transient_http_error,
    sanitize_headers,
    summarize_request_kwargs_for_http_debug,
    tcm_api_error_from_exception,
    parse_tcm_api_error_response,
    post_presigned_batch_with_retry,
    request_with_retry,
)


class TestHttpDebugSanitize(unittest.TestCase):
    def test_sanitize_session_token_header(self):
        h = {"Content-Type": "application/json", "x-tableau-session-token": "secret"}
        s = sanitize_headers(h)
        self.assertEqual(s["Content-Type"], "application/json")
        self.assertEqual(s["x-tableau-session-token"], "***")

    def test_summarize_json_redacts_token(self):
        d = summarize_request_kwargs_for_http_debug({"json": {"token": "pat-value", "x": 1}})
        self.assertEqual(d["json"]["token"], "***")
        self.assertEqual(d["json"]["x"], 1)

    def test_summarize_files_batch(self):
        body = '{"files": ["a/b", "c/d"]}'
        d = summarize_request_kwargs_for_http_debug({"data": body})
        self.assertEqual(d["data"]["files_count"], 2)
        self.assertEqual(d["data"]["first_paths"], ["a/b", "c/d"])


class TestTransient(unittest.TestCase):
    def test_transient_503(self):
        r = requests.Response()
        r.status_code = 503
        err = requests.exceptions.HTTPError()
        err.response = r
        self.assertTrue(is_transient_http_error(err))


@patch("extractor.http_retry.time.sleep", lambda *a, **k: None)
class TestRequestWithRetry(unittest.TestCase):
    def test_503_then_200(self):
        session = MagicMock()
        r1 = requests.Response()
        r1.status_code = 503
        r1.url = "http://x"
        r2 = requests.Response()
        r2.status_code = 200
        r2._content = b"{}"
        r2.url = "http://x"
        session.request.side_effect = [r1, r2]

        events = []

        out = request_with_retry(
            session, "GET", "http://example/api",
            get_headers=lambda: {"x": "y"},
            reauthenticate=None,
            max_total_wait_seconds=60.0,
            http_debug=lambda e: events.append(e),
        )
        self.assertEqual(out.status_code, 200)
        self.assertEqual(session.request.call_count, 2)
        self.assertGreaterEqual(len(events), 4)
        self.assertEqual(events[0].get("http_phase"), "request")
        self.assertEqual(events[1].get("http_phase"), "response")
        self.assertEqual(events[1].get("status_code"), 503)

    def test_401_reauth_retry(self):
        session = MagicMock()
        u = requests.Response()
        u.status_code = 401
        u.url = "http://x"
        ok = requests.Response()
        ok.status_code = 200
        ok._content = b"{}"
        ok.url = "http://x"
        session.request.side_effect = [u, ok]

        def reauth():
            return True

        out = request_with_retry(
            session, "GET", "http://example",
            get_headers=lambda: {"h": "1"},
            reauthenticate=reauth,
            max_total_wait_seconds=60.0,
        )
        self.assertEqual(out.status_code, 200)
        self.assertEqual(session.request.call_count, 2)


@patch("extractor.http_retry.time.sleep", lambda *a, **k: None)
class TestPostPresigned(unittest.TestCase):
    def test_timeout_then_ok(self):
        session = MagicMock()
        t = requests.exceptions.Timeout("t")
        ok = requests.Response()
        ok.status_code = 200
        ok._content = b'{"files":[]}'
        session.request.side_effect = [t, ok]

        r = post_presigned_batch_with_retry(
            session,
            "http://p",
            get_headers=lambda: {"x": "y"},
            data="{}",
            presigned_url_timeout=1.0,
            reauthenticate=None,
            post_timeout_max_total_wait=200.0,
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(session.request.call_count, 2)


class TestTcmApiErrorBody(unittest.TestCase):
    def test_parse_error_json_message_and_code(self):
        r = requests.Response()
        r._content = b'{"message":"Token invalid","errorCode":12,"extra":"x"}'
        r.status_code = 400
        d = parse_tcm_api_error_response(r)
        self.assertEqual(d["message"], "Token invalid")
        self.assertEqual(d["errorCode"], 12)
        self.assertNotIn("extra", d)

    def test_parse_non_json_empty(self):
        r = requests.Response()
        r._content = b"not json"
        r.status_code = 500
        self.assertEqual(parse_tcm_api_error_response(r), {})

    def test_tcm_api_error_from_http_error(self):
        r = requests.Response()
        r._content = b'{"message":"m","errorCode":"E1"}'
        r.status_code = 403
        e = requests.exceptions.HTTPError()
        e.response = r
        d = tcm_api_error_from_exception(e)
        self.assertEqual(d["message"], "m")
        self.assertEqual(d["errorCode"], "E1")
