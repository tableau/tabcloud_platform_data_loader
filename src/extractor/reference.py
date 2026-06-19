"""
Reference data acquisition: pull tenant-wide TCM REST API lists and write
raw JSON pages to extract storage so the transformer can normalize them.

Flow for each ReferenceEntity:
  1. Paginate the entity's api_path endpoint (GET, pageToken loop).
  2. Write each page's raw JSON to:
       reference/{entity.name}/pulled={ISO8601_UTC}/page-{NNNN}.json
  3. Return a ReferenceResult describing what was fetched.

The ``pulled=`` path segment is the pull timestamp (UTC ISO-8601, colons
replaced with hyphens so it is filesystem-safe, e.g.
``pulled=2026-06-15T08-30-00Z``).  The transformer picks the newest
``pulled=`` dir to derive ``snapshot_time`` for the loader watermark.

This module deliberately does NOT import from retriever.py to stay small —
it only uses request_with_retry + _auth_headers from the caller's session.
The LogRetriever calls run_reference_pass() after authenticating.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

from common.reference_entity import ReferenceEntity
from extractor.http_retry import request_with_retry, tcm_api_error_from_exception

import logging

if TYPE_CHECKING:
    import requests as _requests

logger = logging.getLogger("extractor.reference")


@dataclass
class ReferenceResult:
    """Summary of one reference acquisition pass."""
    entity: ReferenceEntity
    pages_written: int = 0
    total_items: int = 0
    pull_ts: str = ""          # e.g. "2026-06-15T08-30-00Z" (filesystem-safe)
    pull_ts_iso: str = ""      # e.g. "2026-06-15T08:30:00Z" (for snapshot_time)
    output_dir: str = ""       # relative path under the extract root
    error: Optional[str] = None
    paths_written: List[str] = field(default_factory=list)


def _filesystem_ts(iso_ts: str) -> str:
    """Convert ISO 8601 timestamp to filesystem-safe form (replace ':' with '-')."""
    return iso_ts.replace(":", "-")


def run_reference_pass(
    entity: ReferenceEntity,
    api_url: str,
    tenant_id: str,
    session: "_requests.Session",
    auth_headers_fn,
    reauth_fn,
    storage_backend,
    op_id: str,
    max_results: int = 100,
    wait_time: float = 60.0,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
    api_retry_max_total_wait_seconds: float = 300.0,
) -> ReferenceResult:
    """
    Paginate one reference entity endpoint and write raw JSON pages to storage.

    :param entity: The ReferenceEntity descriptor.
    :param api_url: Base TCM URL (no trailing slash).
    :param tenant_id: Tenant UUID (from login response).
    :param session: Authenticated requests.Session.
    :param auth_headers_fn: Callable[[], dict] returning current auth headers.
    :param reauth_fn: Callable[[], bool] that re-authenticates and returns success.
    :param storage_backend: StorageBackend instance (extract root).
    :param op_id: Operation ID for logging.
    :param max_results: Page size (default 100).
    :param wait_time: Retry wait seconds.
    :param base_backoff: Initial backoff seconds.
    :param max_backoff: Max backoff cap.
    :param api_retry_max_total_wait_seconds: Total retry budget.
    :returns: ReferenceResult with pages written, total items, paths.
    """
    op_name = f"reference_{entity.name}"
    start_op_time = time.time()
    logger.info("Starting reference acquisition: %s (op=%s)", entity.name, op_id)

    # Resolve the API URL by substituting tenant_id
    endpoint_path = entity.api_path.format(tenant_id=tenant_id)
    url = f"{api_url.rstrip('/')}{endpoint_path}"

    # Pull timestamp: used as both the path segment and snapshot_time
    pull_ts_iso = _utc_now_iso()
    pull_ts_fs = _filesystem_ts(pull_ts_iso)

    # Output directory: reference/{entity.name}/pulled={timestamp}/
    out_dir = f"reference/{entity.name}/pulled={pull_ts_fs}"

    result = ReferenceResult(
        entity=entity,
        pull_ts=pull_ts_fs,
        pull_ts_iso=pull_ts_iso,
        output_dir=out_dir,
    )

    page_token: Optional[str] = None
    page_num = 0

    def _reauth() -> bool:
        return reauth_fn()

    while True:
        params: dict = {"maxResults": max_results}
        if page_token:
            params["pageToken"] = page_token

        try:
            response = request_with_retry(
                session,
                "GET",
                url,
                get_headers=auth_headers_fn,
                reauthenticate=_reauth,
                params=params,
                wait_time=wait_time,
                base_backoff=base_backoff,
                max_backoff=max_backoff,
                max_total_wait_seconds=api_retry_max_total_wait_seconds,
            )
        except Exception as e:
            api_err: dict = {}
            try:
                import requests as _req
                if isinstance(e, _req.exceptions.HTTPError):
                    api_err = tcm_api_error_from_exception(e)
            except ImportError:
                pass
            result.error = str(e)
            duration = int((time.time() - start_op_time) * 1000)
            logger.error(
                "Reference acquisition failed: %s — %s (elapsed %dms)",
                entity.name, e, duration,
            )
            raise

        try:
            data = response.json()
        except ValueError as json_err:
            result.error = f"Non-JSON response: {json_err}"
            duration = int((time.time() - start_op_time) * 1000)
            logger.error(
                "Reference acquisition non-JSON response: %s — %s (elapsed %dms)",
                entity.name, json_err, duration,
            )
            raise RuntimeError(
                f"Non-JSON response from {entity.name} reference endpoint: {json_err}"
            ) from json_err

        items = data.get(entity.items_key, [])
        page_path = f"{out_dir}/page-{page_num:04d}.json"

        storage_backend.write_file(page_path, json.dumps(data, ensure_ascii=False))
        result.paths_written.append(page_path)
        result.pages_written += 1
        result.total_items += len(items)

        logger.debug(
            "Reference %s page %d: %d items (total so far: %d) -> %s",
            entity.name, page_num, len(items), result.total_items, page_path,
        )

        page_token = data.get("pageToken") or data.get("nextPageToken")
        if not page_token:
            break
        page_num += 1

    duration = int((time.time() - start_op_time) * 1000)
    logger.info(
        "Reference acquisition complete: %s — %d pages, %d items, dir=%s (%dms)",
        entity.name, result.pages_written, result.total_items, out_dir, duration,
    )
    return result


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string (seconds precision)."""
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")
