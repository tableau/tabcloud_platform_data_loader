"""
Shared HTTP retry helpers for the Tableau Cloud Manager (TCM) API: transient errors, 401 re-auth, backoff with jitter.
"""

from __future__ import annotations

import copy
import json
import logging
import random
import sys
import time
from typing import Any, Callable, Dict, Optional, Union

import requests

# Optional callback invoked for each HTTP attempt (debug). Receives a plain dict (JSON-serializable).
HttpDebugCallback = Callable[[Dict[str, Any]], None]

_HTTP_DEBUG_BODY_PREVIEW = 2048
_SENSITIVE_HEADER_KEYS = frozenset(
    k.lower()
    for k in (
        "x-tableau-session-token",
        "authorization",
        "cookie",
        "set-cookie",
        "x-tableau-auth",
    )
)
_JSON_REDACT_KEYS = frozenset(k.lower() for k in ("token", "pat_secret", "password", "secret"))


def redact_url(url: str) -> str:
    """Return *url* with the query string removed, suitable for debug logs.

    Presigned URLs embed a time-limited signature in the query string
    (``X-Amz-Signature``, ``X-Goog-Signature``, etc.).  Stripping the query
    string keeps the host and path (sufficient for debugging) without
    persisting the credential in log files.

    Non-URL strings (e.g. already-redacted values) are returned unchanged.
    """
    if not url:
        return url
    idx = url.find("?")
    if idx == -1:
        return url
    return url[:idx] + "?<redacted>"


def sanitize_headers(headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Return a copy of headers safe for logs (secrets redacted)."""
    if not headers:
        return {}
    out: Dict[str, str] = {}
    for k, v in headers.items():
        if k.lower() in _SENSITIVE_HEADER_KEYS:
            out[k] = "***" if v else v
        else:
            out[k] = v
    return out


def _sanitize_json_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            kk: ("***" if kk.lower() in _JSON_REDACT_KEYS else _sanitize_json_obj(vv))
            for kk, vv in obj.items()
        }
    if isinstance(obj, list):
        return [_sanitize_json_obj(x) for x in obj]
    return obj


def summarize_request_kwargs_for_http_debug(request_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Build a log-safe summary of session.request(..., **request_kwargs) extras."""
    summary: Dict[str, Any] = {}
    if "params" in request_kwargs and request_kwargs["params"] is not None:
        summary["params"] = copy.deepcopy(request_kwargs["params"])
    if "json" in request_kwargs and request_kwargs["json"] is not None:
        summary["json"] = _sanitize_json_obj(copy.deepcopy(request_kwargs["json"]))
    if "data" in request_kwargs and request_kwargs["data"] is not None:
        data = request_kwargs["data"]
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
            except (ValueError, TypeError, json.JSONDecodeError):
                summary["data"] = {"kind": "str", "length": len(data), "preview": data[:500]}
            else:
                if isinstance(parsed, dict) and "files" in parsed and isinstance(parsed["files"], list):
                    paths = parsed["files"]
                    summary["data"] = {
                        "kind": "json_files_batch",
                        "files_count": len(paths),
                        "first_paths": paths[:5],
                    }
                else:
                    summary["data"] = {"kind": "json", "body": _sanitize_json_obj(parsed)}
        else:
            summary["data"] = {"kind": type(data).__name__}
    if "timeout" in request_kwargs:
        summary["timeout"] = request_kwargs["timeout"]
    return summary


def _response_error_body_preview(response: requests.Response) -> str:
    try:
        text = response.text
    except Exception:
        return ""
    if len(text) > _HTTP_DEBUG_BODY_PREVIEW:
        return text[:_HTTP_DEBUG_BODY_PREVIEW] + f"... ({len(text)} chars total)"
    return text

# HTTP status codes treated as retryable (transient)
TRANSIENT_STATUS_CODES = frozenset((429, 502, 503, 504))


def parse_tcm_api_error_response(
    response: Optional[requests.Response],
) -> Dict[str, Union[str, int, float, bool, None]]:
    """
    If the body is JSON with 'message' and/or 'errorCode' (typical for TCM API error responses),
    return only those present. Otherwise return an empty dict.
    """
    if response is None:
        return {}
    try:
        data = response.json()
    except (ValueError, TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Union[str, int, float, bool, None]] = {}
    if "message" in data:
        out["message"] = data.get("message")
    if "errorCode" in data:
        out["errorCode"] = data.get("errorCode")
    return out


def tcm_api_error_from_exception(exc: BaseException) -> Dict[str, Union[str, int, float, bool, None]]:
    """Convenience: pull message/errorCode from a failed HTTP request's body when present."""
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        return parse_tcm_api_error_response(exc.response)
    return {}


def print_tcm_api_error_body(err: dict, *, file=None) -> None:
    """Log non-empty message/errorCode (or other) fields from a parsed error dict."""
    if not err:
        return
    message = "TCM API error: " + ", ".join(f"{k}={v!r}" for k, v in err.items())
    if file is sys.stderr:
        logging.getLogger("extractor.http_retry").error(message)
    else:
        logging.getLogger("extractor.http_retry").warning(message)


def is_transient_exception(exc: BaseException) -> bool:
    """True for connection/timeout and similar transport errors."""
    if isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True
    return False


def is_transient_http_error(exc: requests.exceptions.HTTPError) -> bool:
    """True for retryable status codes (when response is present)."""
    resp = exc.response
    if resp is not None and resp.status_code in TRANSIENT_STATUS_CODES:
        return True
    return False


def _sleep_backoff(
    attempt: int,
    *,
    base_seconds: float,
    max_per_sleep: float,
    jitter_seconds: float,
) -> None:
    """Exponential backoff capped at max_per_sleep, plus small jitter."""
    exp = min(max_per_sleep, base_seconds * (2**attempt))
    j = random.uniform(0, jitter_seconds) if jitter_seconds > 0 else 0.0
    time.sleep(exp + j)


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    get_headers: Optional[Callable[[], dict[str, str]]] = None,
    static_headers: Optional[dict[str, str]] = None,
    reauthenticate: Optional[Callable[[], bool]] = None,
    max_reauth: int = 2,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
    jitter_seconds: float = 0.5,
    wait_time: float = 60.0,
    max_total_wait_seconds: float = 300.0,
    max_attempts: int = 24,
    http_debug: Optional[HttpDebugCallback] = None,
    **request_kwargs: Any,
) -> requests.Response:
    """
    Issue a request with retries for transient errors and 401 re-auth.

    * get_headers: called before each attempt to build headers (e.g. fresh session token).
    * static_headers: used as-is for login (no token refresh).
    * reauthenticate: called on 401 after response; should refresh session; retried on success.
    """
    if get_headers is None and static_headers is not None:
        get_headers = lambda: static_headers
    if get_headers is None:
        raise ValueError("Either get_headers or static_headers must be provided")

    reauth_count = 0
    t0 = time.time()
    attempt = 0

    while True:
        if time.time() - t0 > max_total_wait_seconds:
            raise requests.exceptions.RequestException(
                f"Exceeded max_total_wait_seconds={max_total_wait_seconds} for {method} {url!r}"
            )
        if attempt >= max_attempts:
            raise requests.exceptions.RequestException(
                f"Exceeded max_attempts={max_attempts} for {method} {url!r}"
            )

        headers = get_headers()
        if http_debug is not None:
            http_debug(
                {
                    "http_phase": "request",
                    "method": method,
                    "url": url,
                    "attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "headers": sanitize_headers(headers),
                    "request": summarize_request_kwargs_for_http_debug(request_kwargs),
                }
            )
        t_req = time.perf_counter()
        try:
            resp = session.request(method, url, headers=headers, **request_kwargs)
            elapsed_ms = int((time.perf_counter() - t_req) * 1000)
            if http_debug is not None:
                dbg: Dict[str, Any] = {
                    "http_phase": "response",
                    "method": method,
                    "request_url": url,
                    "response_url": getattr(resp, "url", None) or url,
                    "attempt": attempt + 1,
                    "status_code": resp.status_code,
                    "elapsed_ms": elapsed_ms,
                    "reason": resp.reason,
                    "response_headers": sanitize_headers(dict(resp.headers)),
                }
                if not resp.ok:
                    dbg["response_body_preview"] = _response_error_body_preview(resp)
                http_debug(dbg)
            if resp.status_code == 401 and reauthenticate is not None and reauth_count < max_reauth:
                reauth_count += 1
                if reauthenticate():
                    attempt += 1
                    continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            if reauthenticate is not None and e.response is not None and e.response.status_code == 401 and reauth_count < max_reauth:
                reauth_count += 1
                if reauthenticate():
                    attempt += 1
                    continue
            if is_transient_http_error(e):
                code = e.response.status_code if e.response is not None else 0
                if code == 429:
                    sleep_cap = min(wait_time, max_backoff)
                else:
                    sleep_cap = max_backoff
                if http_debug is not None:
                    http_debug(
                        {
                            "http_phase": "retry_scheduled",
                            "method": method,
                            "url": url,
                            "after_attempt": attempt + 1,
                            "reason": f"transient_http status={code}",
                        }
                    )
                _sleep_backoff(attempt, base_seconds=base_backoff, max_per_sleep=sleep_cap, jitter_seconds=jitter_seconds)
                attempt += 1
                continue
            raise
        except requests.exceptions.RequestException as e:
            if http_debug is not None:
                http_debug(
                    {
                        "http_phase": "transport_error",
                        "method": method,
                        "url": url,
                        "attempt": attempt + 1,
                        "elapsed_ms": int((time.perf_counter() - t_req) * 1000),
                        "error_type": type(e).__name__,
                        "error_detail": str(e),
                    }
                )
            if is_transient_exception(e):
                if http_debug is not None:
                    http_debug(
                        {
                            "http_phase": "retry_scheduled",
                            "method": method,
                            "url": url,
                            "after_attempt": attempt + 1,
                            "reason": "transient_transport",
                        }
                    )
                _sleep_backoff(attempt, base_seconds=base_backoff, max_per_sleep=max_backoff, jitter_seconds=jitter_seconds)
                attempt += 1
                continue
            raise


def post_presigned_batch_with_retry(
    session: requests.Session,
    url: str,
    *,
    get_headers: Callable[[], dict[str, str]],
    data: str,
    presigned_url_timeout: float,
    reauthenticate: Optional[Callable[[], bool]] = None,
    wait_time: float = 60.0,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
    jitter_seconds: float = 0.5,
    post_timeout_max_total_wait: float = 120.0,
    post_timeout_max_attempts: int = 8,
    max_total_wait_seconds: float = 300.0,
    http_debug: Optional[HttpDebugCallback] = None,
) -> requests.Response:
    """
    POST presigned-URL batch with:
    - retries on Timeout (deadline: post_timeout_max_total_wait from first timeout)
    - same transient handling as request_with_retry for 429/5xx/connection
    - 401 re-auth
    """
    t_outer = time.time()
    timeout_pass_deadline: Optional[float] = None
    reauth_count = 0
    attempt = 0
    post_timeout_attempt = 0

    while True:
        if time.time() - t_outer > max_total_wait_seconds:
            raise requests.exceptions.RequestException(
                f"Exceeded max_total_wait_seconds={max_total_wait_seconds} for POST presigned batch {url!r}"
            )
        if attempt >= 24:
            raise requests.exceptions.RequestException("Exceeded max attempts for POST presigned batch")

        headers = get_headers()
        if http_debug is not None:
            http_debug(
                {
                    "http_phase": "request",
                    "method": "POST",
                    "url": url,
                    "attempt": attempt + 1,
                    "headers": sanitize_headers(headers),
                    "timeout_seconds": presigned_url_timeout,
                    "request": summarize_request_kwargs_for_http_debug({"data": data}),
                }
            )
        t_req = time.perf_counter()
        try:
            resp = session.request(
                "POST", url, headers=headers, data=data, timeout=presigned_url_timeout
            )
            elapsed_ms = int((time.perf_counter() - t_req) * 1000)
            if http_debug is not None:
                dbg: Dict[str, Any] = {
                    "http_phase": "response",
                    "method": "POST",
                    "request_url": url,
                    "response_url": redact_url(getattr(resp, "url", None) or url),
                    "attempt": attempt + 1,
                    "status_code": resp.status_code,
                    "elapsed_ms": elapsed_ms,
                    "reason": resp.reason,
                    "response_headers": sanitize_headers(dict(resp.headers)),
                }
                if not resp.ok:
                    dbg["response_body_preview"] = _response_error_body_preview(resp)
                http_debug(dbg)
            if resp.status_code == 401 and reauthenticate is not None and reauth_count < 2:
                reauth_count += 1
                if reauthenticate():
                    attempt += 1
                    continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout as e:
            if http_debug is not None:
                http_debug(
                    {
                        "http_phase": "transport_error",
                        "method": "POST",
                        "url": url,
                        "attempt": attempt + 1,
                        "elapsed_ms": int((time.perf_counter() - t_req) * 1000),
                        "error_type": type(e).__name__,
                        "error_detail": str(e),
                    }
                )
            if timeout_pass_deadline is None:
                timeout_pass_deadline = time.time() + post_timeout_max_total_wait
            if time.time() > timeout_pass_deadline or post_timeout_attempt >= post_timeout_max_attempts:
                raise requests.exceptions.RequestException(
                    f"Presigned URL POST: timeout retries exceeded (deadline or max attempts): {e}"
                ) from e
            if http_debug is not None:
                http_debug(
                    {
                        "http_phase": "retry_scheduled",
                        "method": "POST",
                        "url": url,
                        "after_attempt": attempt + 1,
                        "reason": "timeout",
                    }
                )
            _sleep_backoff(
                post_timeout_attempt,
                base_seconds=base_backoff,
                max_per_sleep=max_backoff,
                jitter_seconds=jitter_seconds,
            )
            post_timeout_attempt += 1
            attempt += 1
            continue
        except requests.exceptions.HTTPError as e:
            if reauthenticate is not None and e.response is not None and e.response.status_code == 401 and reauth_count < 2:
                reauth_count += 1
                if reauthenticate():
                    attempt += 1
                    continue
            if is_transient_http_error(e):
                code = e.response.status_code if e.response is not None else 0
                if code == 429:
                    sleep_cap = min(wait_time, max_backoff)
                else:
                    sleep_cap = max_backoff
                if http_debug is not None:
                    http_debug(
                        {
                            "http_phase": "retry_scheduled",
                            "method": "POST",
                            "url": url,
                            "after_attempt": attempt + 1,
                            "reason": f"transient_http status={code}",
                        }
                    )
                _sleep_backoff(attempt, base_seconds=base_backoff, max_per_sleep=sleep_cap, jitter_seconds=jitter_seconds)
                post_timeout_attempt = 0
                attempt += 1
                continue
            raise
        except requests.exceptions.RequestException as e:
            if http_debug is not None:
                http_debug(
                    {
                        "http_phase": "transport_error",
                        "method": "POST",
                        "url": url,
                        "attempt": attempt + 1,
                        "elapsed_ms": int((time.perf_counter() - t_req) * 1000),
                        "error_type": type(e).__name__,
                        "error_detail": str(e),
                    }
                )
            if is_transient_exception(e):
                if http_debug is not None:
                    http_debug(
                        {
                            "http_phase": "retry_scheduled",
                            "method": "POST",
                            "url": url,
                            "after_attempt": attempt + 1,
                            "reason": "transient_transport",
                        }
                    )
                _sleep_backoff(attempt, base_seconds=base_backoff, max_per_sleep=max_backoff, jitter_seconds=jitter_seconds)
                post_timeout_attempt = 0
                attempt += 1
                continue
            raise


def download_get_with_retry(
    session: requests.Session,
    url: str,
    *,
    connect_timeout: float = 30.0,
    read_timeout: float = 120.0,
    max_attempts: int = 3,
    base_backoff: float = 0.5,
    max_backoff: float = 15.0,
    jitter_seconds: float = 0.3,
    http_debug: Optional[HttpDebugCallback] = None,
) -> requests.Response:
    """
    Stream GET to presigned URL with per-attempt timeout and retries.
    Retries on transport errors and 429/5xx. Re-raises immediately on other HTTP errors.
    """
    timeout = (connect_timeout, read_timeout)
    # Presigned URLs embed a signature in the query string; strip it before
    # logging so it is not persisted to the run-log files.
    url_for_log = redact_url(url)
    for attempt in range(max_attempts):
        if http_debug is not None:
            http_debug(
                {
                    "http_phase": "request",
                    "method": "GET",
                    "url": url_for_log,
                    "attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "stream": True,
                    "timeout": {"connect_seconds": connect_timeout, "read_seconds": read_timeout},
                }
            )
        t_req = time.perf_counter()
        try:
            resp = session.get(url, stream=True, timeout=timeout)
            elapsed_ms = int((time.perf_counter() - t_req) * 1000)
            if http_debug is not None:
                dbg: Dict[str, Any] = {
                    "http_phase": "response",
                    "method": "GET",
                    "request_url": url_for_log,
                    "response_url": redact_url(getattr(resp, "url", None) or url),
                    "attempt": attempt + 1,
                    "status_code": resp.status_code,
                    "elapsed_ms": elapsed_ms,
                    "reason": resp.reason,
                    "response_headers": sanitize_headers(dict(resp.headers)),
                }
                if not resp.ok:
                    dbg["response_body_preview"] = _response_error_body_preview(resp)
                http_debug(dbg)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
            if http_debug is not None and not isinstance(e, requests.exceptions.HTTPError):
                http_debug(
                    {
                        "http_phase": "transport_error",
                        "method": "GET",
                        "url": url_for_log,
                        "attempt": attempt + 1,
                        "elapsed_ms": int((time.perf_counter() - t_req) * 1000),
                        "error_type": type(e).__name__,
                        "error_detail": str(e),
                    }
                )
            should_retry = is_transient_exception(e) or (
                isinstance(e, requests.exceptions.HTTPError)
                and e.response is not None
                and e.response.status_code in (429, 500, 502, 503, 504)
            )
            if not should_retry or attempt == max_attempts - 1:
                raise
            if http_debug is not None:
                http_debug(
                    {
                        "http_phase": "retry_scheduled",
                        "method": "GET",
                        "url": url_for_log,
                        "after_attempt": attempt + 1,
                        "reason": "transient",
                    }
                )
            _sleep_backoff(attempt, base_seconds=base_backoff, max_per_sleep=max_backoff, jitter_seconds=jitter_seconds)
