# Retrieve and replicate Tableau Cloud Platform Data API files to local storage.
#
# Supports two dataset profiles:
#   - ACTIVITYLOG: activity-log event files (NDJSON) — existing behaviour, unchanged by default.
#   - SNAPSHOTS:   entity-snapshot Parquet files — site-scoped only at launch.
#
# A DatasetProfile carries the API resource name, filter parameter, and path-segment key so
# that this module never hardcodes "activitylog" or "eventType" in logic.

import concurrent.futures
import json
import logging
import time
import uuid
import datetime
from typing import Dict, List, Optional

import requests

from common.dataset_profile import DatasetProfile, ACTIVITYLOG, SNAPSHOTS
from storage import extract_segment_value_from_path, is_valid_parquet_bytes
from extractor.http_retry import (
    download_get_with_retry,
    print_tcm_api_error_body,
    tcm_api_error_from_exception,
    post_presigned_batch_with_retry,
    request_with_retry,
)
from extractor.result import ExtractionResult
from extractor.path_layout import (
    ExtractPathLayout,
    map_api_to_local_relpath,
    other_layout,
)

logger = logging.getLogger("extractor.retriever")


def get_current_time_iso():
    """Returns the current UTC time in ISO 8601 format."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def create_operation_id():
    """Generates a unique ID for an operation."""
    return str(uuid.uuid4())

def validate_uuid(uuid_string):
    """
    Validates if a string is a valid UUID.

    :param uuid_string: The string to validate
    :return: True if valid UUID, False otherwise
    """
    try:
        uuid.UUID(uuid_string)
        return True
    except ValueError:
        return False

def parse_site_ids(site_ids_string):
    """
    Parses a comma-delimited string of site IDs and validates each one.

    :param site_ids_string: Comma-delimited string of site IDs
    :return: List of validated site IDs, or "all"/"none" sentinel values
    :raises ValueError: If any site ID is invalid or empty
    """
    if not site_ids_string or not site_ids_string.strip():
        raise ValueError("Site IDs cannot be empty")

    # Check for "All" or "None" case (case insensitive)
    lowered_value = site_ids_string.strip().lower()
    if lowered_value == "all":
        return "all"
    if lowered_value == "none":
        return "none"

    # Split by comma and strip whitespace
    site_ids = [site_id.strip() for site_id in site_ids_string.split(',')]

    # Remove empty strings
    site_ids = [site_id for site_id in site_ids if site_id]

    if not site_ids:
        raise ValueError("At least one site ID must be provided")

    # Validate each site ID
    invalid_ids = [site_id for site_id in site_ids if not validate_uuid(site_id)]
    if invalid_ids:
        raise ValueError(f"Invalid UUID format for site IDs: {', '.join(invalid_ids)}")

    return site_ids


def parse_site_uris(site_uri_string):
    """
    Parses a comma-delimited string of site URIs (contentUrl).

    :param site_uri_string: Comma-delimited string of site URIs
    :return: List of site URIs, or "all"/"none" sentinel values
    :raises ValueError: If input is empty after parsing
    """
    if not site_uri_string or not site_uri_string.strip():
        raise ValueError("Site URIs cannot be empty")

    lowered_value = site_uri_string.strip().lower()
    if lowered_value == "all":
        return "all"
    if lowered_value == "none":
        return "none"

    site_uris = [site_uri.strip() for site_uri in site_uri_string.split(',')]
    site_uris = [site_uri for site_uri in site_uris if site_uri]

    if not site_uris:
        raise ValueError("At least one site URI must be provided")

    return site_uris


def parse_event_types(event_types_string):
    """
    Parses a comma-delimited string of event types.

    :param event_types_string: Comma-delimited string of event types (or None/empty)
    :return: None if no filter (do not pass eventType to API), else list of non-empty trimmed strings
    """
    if not event_types_string or not event_types_string.strip():
        return None
    parts = [p.strip() for p in event_types_string.split(",") if p.strip()]
    if not parts:
        return None
    return parts


MAX_CHUNK_HOURS = 168  # 7 days -- API hard limit


def _parse_iso_datetime(dt_str):
    """Parses an ISO 8601 datetime string (with Z or +00:00 offset) into a UTC-aware datetime."""
    dt_str = dt_str.strip()
    if dt_str.endswith('Z'):
        dt_str = dt_str[:-1] + '+00:00'
    return datetime.datetime.fromisoformat(dt_str)


def _to_iso_z(dt):
    """Converts a UTC-aware datetime to an ISO 8601 string with Z suffix."""
    return dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def compute_span_hours(start_time_str, end_time_str):
    """Returns the span between two ISO 8601 timestamps as a float of hours."""
    start = _parse_iso_datetime(start_time_str)
    end = _parse_iso_datetime(end_time_str)
    return (end - start).total_seconds() / 3600


def generate_time_chunks(start_time_str, end_time_str, max_hours=MAX_CHUNK_HOURS):
    """
    Splits an ISO 8601 time range into consecutive chunks of at most max_hours hours.
    max_hours is clamped to MAX_CHUNK_HOURS (168) so no chunk ever exceeds the API limit.
    Returns a list of (chunk_start_iso, chunk_end_iso) string tuples.
    """
    max_hours = min(max_hours, MAX_CHUNK_HOURS)
    start = _parse_iso_datetime(start_time_str)
    end = _parse_iso_datetime(end_time_str)
    delta = datetime.timedelta(hours=max_hours)
    chunks = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + delta, end)
        chunks.append((_to_iso_z(chunk_start), _to_iso_z(chunk_end)))
        chunk_start = chunk_end
    return chunks


def log_operation(operation_id, operation_name, state, inputs=None, result=None, duration=None):
    """
    Logs an operation in a standardized JSON format.

    :param operation_id: A unique ID for the operation.
    :param operation_name: The name of the operation.
    :param state: The state of the operation ('start', 'processing', 'complete').
    :param inputs: A dictionary of key inputs to the operation.
    :param result: A dictionary with the operation result ('success', 'failure') and details.
    :param duration: The duration of the operation in milliseconds.
    """
    log_entry = {
        "operationId": operation_id,
        "operationName": operation_name,
        "startDatetime": get_current_time_iso(),
        "state": state,
    }
    if inputs:
        log_entry["keyInputs"] = inputs
    if result is not None:
        log_entry["result"] = result
    if state == "complete":
        log_entry["endDatetime"] = get_current_time_iso()
        log_entry["durationMs"] = duration

    logging.getLogger("extractor.operation").debug(json.dumps(log_entry, indent=2))


class LogRetriever:
    """
    Core class to handle the retrieval, replication, and logging of Tableau Cloud Manager logs.
    """
    def __init__(
        self,
        pat_name,
        pat_secret,
        api_url,
        site_ids,
        start_time,
        end_time,
        storage_backend,
        wait_time=60,
        page_size=100,
        download_threads=10,
        max_chunk_hours=MAX_CHUNK_HOURS,
        presigned_url_batch_size=100,
        presigned_url_timeout=30,
        log_level="info",
        overwrite=False,
        path_layout=ExtractPathLayout.DATE_FIRST,
        check_alternate_layout=True,
        api_retry_max_total_wait_seconds=300.0,
        post_timeout_max_total_wait=120.0,
        post_timeout_max_attempts=8,
        download_max_attempts=3,
        download_connect_timeout=30.0,
        download_read_timeout=120.0,
        base_backoff_seconds=1.0,
        max_backoff_cap=60.0,
        dataset_profile: DatasetProfile = ACTIVITYLOG,
        snapshot_selection: str = "latest",
        snapshot_cadence: str = "hourly",
        snapshot_lookback_intervals: int = 4,
        snapshot_gap_action: str = "warn",
        allow_tenant_snapshots: bool = False,
    ):
        self.pat_name = pat_name
        self.pat_secret = pat_secret
        self.api_url = api_url.rstrip('/')
        self.session = requests.Session()
        self.session_token = None
        self.tenant_id = None
        self.site_ids = site_ids if isinstance(site_ids, list) else [site_ids]
        self.site_uri_by_id = {}
        self.start_time = start_time
        self.end_time = end_time
        self.storage_backend = storage_backend
        self.wait_time = wait_time
        self.page_size = page_size
        self.download_threads = download_threads
        self.max_chunk_hours = min(max_chunk_hours, MAX_CHUNK_HOURS)
        self.presigned_url_batch_size = presigned_url_batch_size
        self.presigned_url_timeout = presigned_url_timeout
        self.log_level = log_level.lower()
        self.overwrite = overwrite
        self.path_layout = path_layout
        self.check_alternate_layout = check_alternate_layout
        self.api_retry_max_total_wait_seconds = float(api_retry_max_total_wait_seconds)
        self.post_timeout_max_total_wait = float(post_timeout_max_total_wait)
        self.post_timeout_max_attempts = int(post_timeout_max_attempts)
        self.download_max_attempts = int(download_max_attempts)
        self.download_connect_timeout = float(download_connect_timeout)
        self.download_read_timeout = float(download_read_timeout)
        self.base_backoff_seconds = float(base_backoff_seconds)
        self.max_backoff_cap = float(max_backoff_cap)
        # Dataset profile (ACTIVITYLOG or SNAPSHOTS)
        self.profile: DatasetProfile = dataset_profile
        # Snapshot-specific options (only meaningful when profile is SNAPSHOTS)
        self.snapshot_selection: str = snapshot_selection  # "latest" | "all"
        self.snapshot_cadence: str = snapshot_cadence      # "hourly" | "daily"
        self.snapshot_lookback_intervals: int = max(1, int(snapshot_lookback_intervals))
        self.snapshot_gap_action: str = snapshot_gap_action  # "warn" | "error" | "ignore"
        self.allow_tenant_snapshots: bool = allow_tenant_snapshots
        # Last TCM API error body fields (e.g. message, errorCode) from the most recent failed call.
        self._last_tcm_api_error: dict = {}

    def _http_debug_callback(self, operation_id, operation_name):
        """
        When log_level is debug, return a callback that logs each HTTP attempt as JSON
        under the given operation (same operationId as the surrounding step).
        """
        if self.log_level != "debug":
            return None

        def _cb(event: dict):
            self._log(operation_id, operation_name, "processing", result={"status": "http", **event})

        return _cb

    def _log(self, operation_id, operation_name, state, inputs=None, result=None, duration=None):
        """Helper to call the logging function."""
        log_operation(operation_id, operation_name, state, inputs, result, duration)

    def _site_log_label(self, site_id: str) -> str:
        """Return site URI when known, otherwise fall back to site UUID."""
        site_uri = str(self.site_uri_by_id.get(site_id, "")).strip()
        return site_uri or site_id

    def _auth_headers(self):
        return {
            "Content-Type": "application/json",
            "x-tableau-session-token": self.session_token,
        }

    def _reauth(self, op_id) -> bool:
        """Re-login on 401; used by request_with_retry."""
        return self._login(op_id)

    def _login(self, op_id):
        """Authenticates with the API using PAT and retrieves a session token."""
        op_name = "login"
        step_start_time = time.time()
        self._last_tcm_api_error = {}
        self._log(op_id, op_name, "start", inputs={"pat_name": self.pat_name})

        url = f"{self.api_url}/api/v1/pat/login"
        headers = {"Content-Type": "application/json"}
        payload = {
            "token": self.pat_secret
        }

        try:
            response = request_with_retry(
                self.session,
                "POST",
                url,
                static_headers=headers,
                reauthenticate=None,
                json=payload,
                wait_time=self.wait_time,
                base_backoff=self.base_backoff_seconds,
                max_backoff=self.max_backoff_cap,
                max_total_wait_seconds=self.api_retry_max_total_wait_seconds,
                http_debug=self._http_debug_callback(op_id, op_name),
            )
            response_text = response.json()
            self.session_token = response_text.get("sessionToken")
            self.tenant_id = response_text.get("tenantId")

            duration = int((time.time() - step_start_time) * 1000)
            self._log(op_id, op_name, "complete", result={"status": "success"}, duration=duration)
            return True
        except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
            duration = int((time.time() - step_start_time) * 1000)
            self._last_tcm_api_error = tcm_api_error_from_exception(e)
            err_result = {"status": "failure", "details": str(e), **self._last_tcm_api_error}
            self._log(op_id, op_name, "complete", result=err_result, duration=duration)
            return False
        except (ValueError, json.JSONDecodeError) as e:
            duration = int((time.time() - step_start_time) * 1000)
            self._log(op_id, op_name, "complete", result={"status": "failure", "details": str(e)}, duration=duration)
            return False

    def _list_tenant_sites(self, op_id):
        """
        Retrieves all sites from the tenant using the List Tenant Sites API.
        This call is subject to API throttling.
        """
        op_name = "list_tenant_sites"
        start_op_time = time.time()
        self._log(op_id, op_name, "start", inputs={})

        def reauth() -> bool:
            return self._reauth(op_id)

        all_sites = []
        page_token = None
        max_results = 100  # Reasonable page size

        while True:
            url = f"{self.api_url}/api/v1/tenants/{self.tenant_id}/sites"
            params = {"maxResults": max_results}
            if page_token:
                params["pageToken"] = page_token

            try:
                response = request_with_retry(
                    self.session,
                    "GET",
                    url,
                    get_headers=lambda: self._auth_headers(),
                    reauthenticate=reauth,
                    params=params,
                    wait_time=self.wait_time,
                    base_backoff=self.base_backoff_seconds,
                    max_backoff=self.max_backoff_cap,
                    max_total_wait_seconds=self.api_retry_max_total_wait_seconds,
                    http_debug=self._http_debug_callback(op_id, op_name),
                )
            except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
                duration = int((time.time() - start_op_time) * 1000)
                api: dict = tcm_api_error_from_exception(e) if isinstance(
                    e, requests.exceptions.HTTPError
                ) else {}
                self._log(
                    op_id, op_name, "complete",
                    result={"status": "failure", "details": str(e), **api},
                    duration=duration,
                )
                raise

            try:
                response_data = response.json()
            except ValueError as json_err:
                duration = int((time.time() - start_op_time) * 1000)
                self._log(
                    op_id, op_name, "complete",
                    result={"status": "failure", "details": f"Non-JSON response: {json_err}"},
                    duration=duration,
                )
                raise RuntimeError(f"Non-JSON response from list_tenant_sites: {json_err}") from json_err

            sites = response_data.get("sites", [])
            all_sites.extend(sites)

            page_token = response_data.get("pageToken")
            if not page_token:
                break

            self._log(
                op_id, op_name, "processing",
                result={"status": "success", "sites_count": len(sites), "total_sites": len(all_sites)},
            )

        duration = int((time.time() - start_op_time) * 1000)
        self._log(
            op_id, op_name, "complete",
            result={"status": "success", "total_sites": len(all_sites)},
            duration=duration,
        )
        return all_sites

    def _list_files(self, op_id, start_time, end_time, event_type=None, site_id=None):
        """
        Retrieves a list of log files from the API for a specific site or tenant, handling pagination.
        This call is subject to API throttling.
        """
        op_name = "list_files"
        start_op_time = time.time()
        inputs = {"start_time": start_time, "end_time": end_time, "event_type": event_type, "site_id": site_id}
        self._log(op_id, op_name, "start", inputs=inputs)

        def reauth() -> bool:
            return self._reauth(op_id)

        all_files = []
        page_token = None

        resource = self.profile.resource
        if site_id:
            url = f"{self.api_url}/api/v1/tenants/{self.tenant_id}/sites/{site_id}/{resource}"
        else:
            url = f"{self.api_url}/api/v1/tenants/{self.tenant_id}/{resource}"

        while True:
            params = {
                "startTime": start_time,
                "endTime": end_time,
                "pageSize": self.page_size
            }
            if page_token:
                params["pageToken"] = page_token
            if event_type is not None:
                params[self.profile.filter_param] = event_type

            try:
                response = request_with_retry(
                    self.session,
                    "GET",
                    url,
                    get_headers=lambda: self._auth_headers(),
                    reauthenticate=reauth,
                    params=params,
                    wait_time=self.wait_time,
                    base_backoff=self.base_backoff_seconds,
                    max_backoff=self.max_backoff_cap,
                    max_total_wait_seconds=self.api_retry_max_total_wait_seconds,
                    http_debug=self._http_debug_callback(op_id, op_name),
                )
            except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
                duration = int((time.time() - start_op_time) * 1000)
                api: dict = tcm_api_error_from_exception(e) if isinstance(
                    e, requests.exceptions.HTTPError
                ) else {}
                self._log(
                    op_id, op_name, "complete",
                    result={
                        "status": "failure",
                        "details": str(e),
                        "site_id": site_id,
                        **api,
                    },
                    duration=duration,
                )
                raise

            try:
                response_data = response.json()
            except ValueError as json_err:
                duration = int((time.time() - start_op_time) * 1000)
                self._log(
                    op_id, op_name, "complete",
                    result={"status": "failure", "details": f"Non-JSON response: {json_err}", "site_id": site_id},
                    duration=duration,
                )
                raise RuntimeError(f"Non-JSON response from list_files: {json_err}") from json_err

            files = response_data.get("files", [])
            all_files.extend(files)

            page_token = response_data.get("pageToken")

            self._log(
                op_id, op_name, "processing",
                result={"status": "success", "file_count": len(files), "site_id": site_id},
            )

            if not page_token:
                break

        duration = int((time.time() - start_op_time) * 1000)
        self._log(
            op_id, op_name, "complete",
            result={"status": "success", "total_file_count": len(all_files), "site_id": site_id},
            duration=duration,
        )
        return all_files

    def _get_file_list_for_event_types(self, op_id, start_time, end_time, event_types, site_id=None, max_chunk_hours=MAX_CHUNK_HOURS):
        """
        Returns a file list for the given scope (tenant or site), applying event-type logic per
        time chunk. The full time range is split into chunks of at most max_chunk_hours hours so
        that no single API call exceeds the 7-day (168-hour) limit.

        Event-type logic per chunk:
        - None or empty: one _list_files call with no eventType param.
        - One event type: one _list_files call with that eventType.
        - 2-5 event types: one _list_files call per event type; merge and dedupe by path.
        - 6+ event types: one _list_files call with no eventType; filter by path (eventType= segment).

        Results across all chunks are merged and deduplicated by file path.
        """
        chunks = generate_time_chunks(start_time, end_time, max_hours=max_chunk_hours)
        if len(chunks) > 1:
            self._log(op_id, "get_file_list_for_event_types", "processing", result={
                "status": "chunking",
                "total_chunks": len(chunks),
                "max_chunk_hours": max_chunk_hours,
                "site_id": site_id,
            })

        seen_paths = set()
        all_files = []

        for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
            if len(chunks) > 1:
                logger.info(f"Fetching file list chunk {chunk_idx + 1}/{len(chunks)}: {chunk_start} to {chunk_end}")

            if not event_types:
                chunk_files = self._list_files(op_id, chunk_start, chunk_end, event_type=None, site_id=site_id)
            elif len(event_types) == 1:
                chunk_files = self._list_files(op_id, chunk_start, chunk_end, event_type=event_types[0], site_id=site_id)
            elif len(event_types) <= 5:
                chunk_files = []
                for et in event_types:
                    for f in self._list_files(op_id, chunk_start, chunk_end, event_type=et, site_id=site_id):
                        if f.get("path") not in seen_paths:
                            chunk_files.append(f)
            else:
                # 6+ filter values: fetch all, filter by path segment
                filter_value_set = set(event_types)
                raw = self._list_files(op_id, chunk_start, chunk_end, event_type=None, site_id=site_id)
                chunk_files = [
                    f for f in raw
                    if extract_segment_value_from_path(f.get("path") or "", self.profile.path_key) in filter_value_set
                ]

            for f in chunk_files:
                path = f.get("path")
                if path and path not in seen_paths:
                    seen_paths.add(path)
                    all_files.append(f)

        return all_files

    def _get_presigned_urls(self, op_id, file_paths, site_id=None):
        """
        Generator: yields each batch's presigned URL list as soon as it is returned by the API,
        allowing the caller to dispatch downloads immediately rather than waiting for all batches.
        This call is subject to API throttling.
        Batches requests using self.presigned_url_batch_size (separate from list_files page_size).
        """
        op_name = "get_presigned_urls"
        start_op_time = time.time()
        total_batches = (len(file_paths) + self.presigned_url_batch_size - 1) // self.presigned_url_batch_size
        inputs = {
            "file_count": len(file_paths),
            "batch_size": self.presigned_url_batch_size,
            "total_batches": total_batches,
            "timeout_seconds": self.presigned_url_timeout,
            "site_id": site_id,
        }
        self._log(op_id, op_name, "start", inputs=inputs)

        # Determine URL based on whether this is a tenant or site call
        resource = self.profile.resource
        if site_id:
            url = f"{self.api_url}/api/v1/tenants/{self.tenant_id}/sites/{site_id}/{resource}"
        else:
            url = f"{self.api_url}/api/v1/tenants/{self.tenant_id}/{resource}"

        def reauth() -> bool:
            return self._reauth(op_id)

        total_url_count = 0
        batch_elapsed_ms_history = []
        logged_complete = False

        try:
            for batch_num, i in enumerate(range(0, len(file_paths), self.presigned_url_batch_size), start=1):
                batch_paths = file_paths[i:i + self.presigned_url_batch_size]
                payload = json.dumps({"files": batch_paths})
                batch_start_datetime = get_current_time_iso()
                batch_start = time.time()

                self._log(op_id, op_name, "processing", result={
                    "status": "batch_request",
                    "batch": batch_num,
                    "total_batches": total_batches,
                    "paths_in_request": len(batch_paths),
                    "site_id": site_id,
                })

                if self.log_level == "debug":
                    self._log(op_id, op_name, "processing", result={
                        "status": "batch_start_debug",
                        "batch": batch_num,
                        "total_batches": total_batches,
                        "batch_start_datetime": batch_start_datetime,
                        "file_paths": batch_paths,
                        "site_id": site_id,
                    })

                try:
                    response = post_presigned_batch_with_retry(
                        self.session,
                        url,
                        get_headers=lambda: self._auth_headers(),
                        data=payload,
                        presigned_url_timeout=self.presigned_url_timeout,
                        reauthenticate=reauth,
                        wait_time=self.wait_time,
                        base_backoff=self.base_backoff_seconds,
                        max_backoff=self.max_backoff_cap,
                        post_timeout_max_total_wait=self.post_timeout_max_total_wait,
                        post_timeout_max_attempts=self.post_timeout_max_attempts,
                        max_total_wait_seconds=self.api_retry_max_total_wait_seconds,
                        http_debug=self._http_debug_callback(op_id, op_name),
                    )
                except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
                    duration = int((time.time() - start_op_time) * 1000)
                    logged_complete = True
                    api: dict = tcm_api_error_from_exception(e) if isinstance(
                        e, requests.exceptions.HTTPError
                    ) else {}
                    self._log(op_id, op_name, "complete", result={
                        "status": "failure",
                        "details": str(e),
                        "paths_in_request": len(batch_paths),
                        "batch_start_datetime": batch_start_datetime,
                        "batch_end_datetime": get_current_time_iso(),
                        "total_elapsed_seconds": round(duration / 1000, 1),
                        "site_id": site_id,
                        **api,
                    }, duration=duration)
                    raise

                urls_data = response.json().get("files", [])

                total_url_count += len(urls_data)
                batch_end_datetime = get_current_time_iso()
                batch_elapsed_ms = int((time.time() - batch_start) * 1000)
                batch_elapsed_ms_history.append(batch_elapsed_ms)
                avg_ms = sum(batch_elapsed_ms_history) / len(batch_elapsed_ms_history)
                remaining_batches = total_batches - batch_num
                eta_seconds = int(avg_ms * remaining_batches / 1000)

                self._log(op_id, op_name, "processing", result={
                    "status": "batch_complete",
                    "batch": batch_num,
                    "total_batches": total_batches,
                    "batch_size": len(batch_paths),
                    "batch_start_datetime": batch_start_datetime,
                    "batch_end_datetime": batch_end_datetime,
                    "batch_elapsed_ms": batch_elapsed_ms,
                    "urls_so_far": total_url_count,
                    "eta_seconds": eta_seconds,
                    "site_id": site_id,
                })

                yield urls_data

        finally:
            if not logged_complete:
                duration = int((time.time() - start_op_time) * 1000)
                self._log(op_id, op_name, "complete", result={
                    "status": "success",
                    "url_count": total_url_count,
                    "total_elapsed_seconds": round(duration / 1000, 1),
                    "site_id": site_id,
                }, duration=duration)

    def _download_file(self, url, file_path, relative_path):
        """Downloads a file from a pre-signed URL to storage (retries in download_get_with_retry)."""
        op_name = "download_file"
        start_time = time.time()
        op_id = None
        if self.log_level == "debug":
            op_id = create_operation_id()
            self._log(op_id, op_name, "start", inputs={"file_path": file_path})

        try:
            http_dbg = self._http_debug_callback(op_id, op_name) if op_id is not None else None
            response = download_get_with_retry(
                self.session,
                url,
                connect_timeout=self.download_connect_timeout,
                read_timeout=self.download_read_timeout,
                max_attempts=self.download_max_attempts,
                base_backoff=0.5,
                max_backoff=15.0,
                http_debug=http_dbg,
            )
            chunks = []
            for chunk in response.iter_content(chunk_size=8192):
                chunks.append(chunk)
            data = b"".join(chunks)

            # For Parquet files (entity snapshots): validate the magic bytes before
            # persisting.  A truncated or incomplete download will be missing the
            # trailing PAR1 footer; rejecting it here lets the existing second-pass
            # retry mechanism re-download the file cleanly.
            if self.profile.files_are_parquet:
                expected_len = None
                cl_header = response.headers.get("Content-Length") or response.headers.get("content-length")
                if cl_header:
                    try:
                        expected_len = int(cl_header)
                    except (ValueError, TypeError):
                        pass
                size_mismatch = expected_len is not None and len(data) != expected_len
                magic_invalid = not is_valid_parquet_bytes(data)
                if size_mismatch or magic_invalid:
                    reason = (
                        f"size mismatch (expected {expected_len}, got {len(data)})"
                        if size_mismatch
                        else f"invalid Parquet magic bytes (got header={data[:4]!r}, footer={data[-4:]!r})"
                    )
                    logger.warning(
                        "Parquet integrity check failed for %s — %s. "
                        "File will NOT be written; the second-pass retry will re-download it.",
                        relative_path, reason,
                    )
                    return None

            self.storage_backend.write_bytes(relative_path, data)

            duration = int((time.time() - start_time) * 1000)
            if self.log_level == "debug":
                self._log(op_id, op_name, "complete", result={"status": "success", "dest": self.storage_backend.uri(relative_path)}, duration=duration)
            return relative_path
        except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
            duration = int((time.time() - start_time) * 1000)
            if self.log_level == "debug" and op_id is not None:
                self._log(op_id, op_name, "complete", result={"status": "failure", "details": str(e)}, duration=duration)
            return None

    def _local_rel_path(self, api_path: str) -> str:
        return map_api_to_local_relpath(api_path, self.path_layout, self.profile.path_key)

    def _file_present(self, api_path: str) -> bool:
        """Check if a file already exists in storage (primary or alternate layout)."""
        try:
            primary_rel = map_api_to_local_relpath(api_path, self.path_layout, self.profile.path_key)
        except ValueError:
            return False
        if self.storage_backend.exists(primary_rel):
            return True
        if not self.check_alternate_layout:
            return False
        try:
            alt_rel = map_api_to_local_relpath(api_path, other_layout(self.path_layout), self.profile.path_key)
        except ValueError:
            return False
        return self.storage_backend.exists(alt_rel)

    def _download_and_replicate(self, url_info):
        """Downloads a single file to storage via the backend. Intended for concurrent use."""
        url = url_info.get('url')
        path = url_info.get('path')
        if not url or not path:
            logger.warning(f"Presigned URL entry missing 'url' or 'path'; skipping: {url_info}")
            return False

        try:
            local_rel = self._local_rel_path(path)
        except ValueError as e:
            logger.error("Cannot map path to local layout: %s", e)
            return False
        if not self.overwrite and self._file_present(path):
            logger.info("File already in storage, skipping: %r", path)
            return True
        result = self._download_file(url, path, local_rel)
        if not result:
            return False
        if self.log_level == "debug":
            logger.debug("File downloaded to: %s", self.storage_backend.uri(local_rel))
        return True

    def _run_download_pass(self, op_id, paths_to_sign, site_id, second_pass: bool) -> None:
        """Download all paths in paths_to_sign using presigned URL batches and thread pool."""
        if not paths_to_sign:
            return
        if second_pass:
            logger.info("Second pass: re-fetching presigned URLs for %d path(s)...", len(paths_to_sign))
            self._log(op_id, "process_files_batch", "processing", result={
                "status": "second_pass",
                "path_count": len(paths_to_sign),
                "site_id": site_id,
            })
        all_futures = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.download_threads) as executor:
            for urls_data in self._get_presigned_urls(op_id, paths_to_sign, site_id):
                for url_info in urls_data:
                    fut = executor.submit(self._download_and_replicate, url_info)
                    all_futures[fut] = url_info
            for future in concurrent.futures.as_completed(all_futures):
                try:
                    future.result()
                except Exception as e:
                    url_info = all_futures[future]
                    logger.error("Unexpected error processing %s: %s", url_info.get("path"), e)

    def _process_files(self, file_list, op_id, site_id=None):
        """Download and replicate files; may run a second pass for failures.

        Returns dict with failed_paths (still missing in storage) and expected fetch count.
        """
        if not file_list:
            logger.info("No files to process.")
            return {"failed_paths": [], "expected": 0}

        op_name = "process_files_batch"
        start_time = time.time()
        inputs = {
            "file_count": len(file_list),
            "site_id": site_id,
            "download_threads": self.download_threads,
            "overwrite": self.overwrite,
        }
        self._log(op_id, op_name, "start", inputs=inputs)

        to_fetch = []
        already_present = []
        missing_path_count = 0

        if self.overwrite:
            for f in file_list:
                path = f.get('path')
                if not path:
                    missing_path_count += 1
                    continue
                to_fetch.append(path)
        else:
            from storage.presence import build_presence_set
            mapped_paths = {}
            for f in file_list:
                path = f.get('path')
                if not path:
                    missing_path_count += 1
                    continue
                try:
                    local_rel = self._local_rel_path(path)
                    mapped_paths[path] = local_rel
                except ValueError as e:
                    logger.error("Cannot map path to local layout: %s; skipping: %r", e, path)
                    missing_path_count += 1

            candidate_rels = list(mapped_paths.values())
            existing = build_presence_set(
                self.storage_backend,
                candidate_rels,
                layout=self.path_layout.value,
                max_threads=self.download_threads,
                partition_key=self.profile.path_key,
            )

            for api_path, local_rel in mapped_paths.items():
                if local_rel in existing:
                    already_present.append(api_path)
                else:
                    to_fetch.append(api_path)

        if missing_path_count:
            logger.warning("%d file entries missing 'path'; skipped.", missing_path_count)

        pre_filter_result = {
            "status": "pre_filter",
            "files_to_fetch": len(to_fetch),
            "files_already_present": len(already_present),
            "site_id": site_id,
        }
        if self.log_level == "debug" and already_present:
            pre_filter_result["already_present_paths"] = already_present
        self._log(op_id, op_name, "processing", result=pre_filter_result)

        if not to_fetch:
            logger.info("All files already present in storage; nothing to fetch.")
            duration = int((time.time() - start_time) * 1000)
            self._log(op_id, op_name, "complete", result={
                "status": "success",
                "replicated_count": 0,
                "skipped_count": len(already_present),
                "site_id": site_id,
            }, duration=duration)
            return {"failed_paths": [], "expected": 0}

        self._run_download_pass(op_id, to_fetch, site_id, second_pass=False)
        still_failed = [p for p in to_fetch if not self._file_present(p)]
        if still_failed:
            self._run_download_pass(op_id, still_failed, site_id, second_pass=True)
        final_failed = [p for p in to_fetch if not self._file_present(p)]
        batch_status = "success" if not final_failed else "degraded"
        replicated_ok = len(to_fetch) - len(final_failed)
        duration = int((time.time() - start_time) * 1000)
        complete_result = {
            "status": batch_status,
            "replicated_count": replicated_ok,
            "expected_fetch_count": len(to_fetch),
            "failed_paths": final_failed,
            "skipped_count": len(already_present),
            "site_id": site_id,
        }
        self._log(op_id, op_name, "complete", result=complete_result, duration=duration)
        return {"failed_paths": final_failed, "expected": len(to_fetch)}

    # ------------------------------------------------------------------
    # Snapshot-specific helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _path_to_date_tuple(path: str) -> Optional[tuple]:
        """Derive a sortable (year, month, day, hour) tuple from a path's y=/m=/d=/h= segments.

        Returns None if date segments are missing.  Hour defaults to 0 when absent.
        """
        def _seg(key: str) -> Optional[str]:
            needle = f"{key}="
            idx = path.find(needle)
            if idx < 0:
                return None
            start = idx + len(needle)
            end = path.find("/", start)
            return path[start:] if end < 0 else path[start:end]

        y, m, d = _seg("y"), _seg("m"), _seg("d")
        if y is None or m is None or d is None:
            return None
        h = _seg("h") or "0"
        try:
            return (int(y), int(m), int(d), int(h))
        except ValueError:
            return None

    def _select_latest_per_partition(self, file_list: List[dict]) -> List[dict]:
        """
        Given a flat list of file entries, keep only the file(s) that belong to the
        *newest* snapshot for each (site_id, partition-key) pair (e.g., each
        entityType within each site).

        Files are grouped by the combination of the ``_site_id`` tag (added during
        cross-site collection) and the ``self.profile.path_key`` value in their path.
        When ``_site_id`` is absent (e.g. tenant-scope callers), grouping falls back
        to path-key only so that each site independently retains its own newest hour.
        Within each group the newest is determined by the y=/m=/d=/h= date block.
        Files whose date cannot be parsed are preserved (not dropped).

        This is only used when profile is SNAPSHOTS and snapshot_selection == "latest".
        In "latest-only" extract mode the result is a no-op (only one snapshot per type
        is on disk); in retain-all mode it actively selects the newest.
        """
        path_key = self.profile.path_key
        groups: Dict[tuple, List[dict]] = {}
        undated: List[dict] = []

        for entry in file_list:
            path = entry.get("path") or ""
            pv = extract_segment_value_from_path(path, path_key)
            date_key = self._path_to_date_tuple(path)
            if pv is None or date_key is None:
                undated.append(entry)
                continue
            site_id = entry.get("_site_id")  # None for tenant-scope entries
            group_key = (site_id, pv)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append((date_key, entry))

        result: List[dict] = list(undated)
        for group_key, items in groups.items():
            best_key = max(item[0] for item in items)
            result.extend(entry for (dk, entry) in items if dk == best_key)
        return result

    def _cadence_default_window(self) -> tuple:
        """Compute a (start_time, end_time) ISO string pair for the snapshots cadence window.

        Uses snapshot_cadence and snapshot_lookback_intervals:
          - hourly: now - N hours to now
          - daily:  now - N days to now
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        if self.snapshot_cadence == "daily":
            delta = datetime.timedelta(days=self.snapshot_lookback_intervals)
        else:
            delta = datetime.timedelta(hours=self.snapshot_lookback_intervals)
        start = now - delta
        return _to_iso_z(start), _to_iso_z(now)

    def _check_delivery_gaps(
        self,
        file_list: List[dict],
        requested_filter_values: Optional[List[str]],
    ) -> None:
        """
        Warn, error, or silently skip when expected snapshots are missing or stale.

        - If specific filter values (entity types) were requested, check each is present
          across at least one site.
        - Additionally check that each (site, entity type) pair has a snapshot fresher
          than 1 cadence interval (hourly = 2 h grace, daily = 2 d grace). Site labels
          are included in gap messages so a lagging site is visible even when another
          site has a fresh snapshot for the same entity type.
        - Action is controlled by self.snapshot_gap_action: "warn" | "error" | "ignore".
        """
        if self.snapshot_gap_action == "ignore":
            return

        path_key = self.profile.path_key
        now = datetime.datetime.now(datetime.timezone.utc)
        grace_factor = 2  # allow up to 2 intervals of grace before flagging stale
        if self.snapshot_cadence == "daily":
            grace_delta = datetime.timedelta(days=grace_factor)
        else:
            grace_delta = datetime.timedelta(hours=grace_factor)
        stale_threshold = now - grace_delta

        # Build: (site_id, entity_type) -> newest date tuple (None if path has no date segments).
        # site_id is None for tenant-scope entries.
        # Track all seen (site_id, entity_type) pairs regardless of whether a date was parsed,
        # so the presence check below is not falsely tripped by undated paths.
        newest: Dict[tuple, Optional[tuple]] = {}
        for entry in file_list:
            path = entry.get("path") or ""
            pv = extract_segment_value_from_path(path, path_key)
            if pv is None:
                continue
            date_tup = self._path_to_date_tuple(path)
            site_id = entry.get("_site_id")
            key = (site_id, pv)
            if key not in newest:
                newest[key] = date_tup
            elif date_tup is not None and (newest[key] is None or date_tup > newest[key]):
                newest[key] = date_tup

        gaps: List[str] = []

        if requested_filter_values:
            found_entity_types = {pv for (_, pv) in newest}
            for fv in requested_filter_values:
                if fv not in found_entity_types:
                    gaps.append(f"{fv}: no snapshot found in the lookback window (any site)")

        for (site_id, pv), date_tup in newest.items():
            if date_tup is None:
                continue
            y, m, d, h = date_tup
            dt = datetime.datetime(y, m, d, h, tzinfo=datetime.timezone.utc)
            if dt < stale_threshold:
                site_label = self._site_log_label(site_id) if site_id else "tenant"
                gaps.append(
                    f"{pv} [{site_label}]: newest snapshot is {dt.isoformat()} "
                    f"(older than {grace_factor}x{self.snapshot_cadence} grace window)"
                )

        if gaps:
            msg = (
                f"Snapshot delivery gap(s) detected "
                f"(cadence={self.snapshot_cadence}, "
                f"lookback={self.snapshot_lookback_intervals}):\n"
                + "\n".join(f"  - {g}" for g in gaps)
            )
            if self.snapshot_gap_action == "error":
                raise RuntimeError(msg)
            logger.warning(msg)

    def run_incremental(self, event_types=None, retrieve_tenant_logs=False) -> ExtractionResult:
        """Retrieve and replicate files from tenant and/or sites.

        For ACTIVITYLOG profiles, ``event_types`` is the list of eventType values to filter;
        ``retrieve_tenant_logs`` controls whether tenant-level files are also fetched.

        For SNAPSHOTS profiles, ``event_types`` is interpreted as entity types to filter.
        Tenant scope is always skipped for snapshots (not yet available at launch) unless
        ``allow_tenant_snapshots`` was set at construction.  A cadence-aware lookback window
        is used for the time range unless start_time/end_time were explicitly set.

        Note: ``retrieve_tenant_logs`` now defaults to **False**.  Callers that previously
        relied on the implicit tenant retrieval must now pass ``retrieve_tenant_logs=True``
        explicitly.  See AGENTS.md for the behavior-change note.
        """
        op_id = create_operation_id()
        op_name = "run_incremental"
        start_time_clock = time.time()
        all_failed: list = []
        total_expected = 0

        is_snapshots = (self.profile is SNAPSHOTS or self.profile.name == "snapshots")

        # For snapshots, use a cadence-aware window if no explicit window was provided.
        if is_snapshots and not self.start_time and not self.end_time:
            window_start, window_end = self._cadence_default_window()
            logger.info(
                "Snapshots: no explicit time window provided; using cadence-aware default "
                "(%s x %d intervals): %s to %s",
                self.snapshot_cadence, self.snapshot_lookback_intervals, window_start, window_end,
            )
        else:
            window_start = self.start_time
            window_end = self.end_time

        span_hours = compute_span_hours(window_start, window_end)
        chunks = generate_time_chunks(window_start, window_end, max_hours=self.max_chunk_hours)
        self._log(op_id, op_name, "start", inputs={
            "dataset": self.profile.name,
            "filter_values": event_types,
            "retrieve_tenant_logs": retrieve_tenant_logs,
            "start_time": window_start,
            "end_time": window_end,
            "span_hours": round(span_hours, 2),
            "max_chunk_hours": self.max_chunk_hours,
            "api_request_chunks": len(chunks),
            "snapshot_selection": self.snapshot_selection if is_snapshots else None,
        })
        logger.info(
            "Date range spans %.1f days (%.1f hours); will issue %d API chunk(s) of up to %d hours each.",
            span_hours / 24, span_hours, len(chunks), self.max_chunk_hours,
        )

        if not self.session_token and not self._login(op_id):
            duration = int((time.time() - start_time_clock) * 1000)
            fail: dict = {"status": "failed", "details": "login failed"}
            fail.update(self._last_tcm_api_error)
            self._log(op_id, op_name, "complete", result=fail, duration=duration)
            if self._last_tcm_api_error:
                print_tcm_api_error_body(self._last_tcm_api_error)
            return ExtractionResult(status="failed", note="login failed")

        # ---- Tenant scope ----
        if is_snapshots:
            # Tenant-level snapshots are not yet available from the API.
            if retrieve_tenant_logs:
                if self.allow_tenant_snapshots:
                    logger.info("Retrieving tenant-level snapshots (experimental; --allow-tenant-snapshots is set)...")
                    tenant_file_list = self._get_file_list_for_event_types(
                        op_id, window_start, window_end, event_types,
                        site_id=None, max_chunk_hours=self.max_chunk_hours,
                    )
                    if is_snapshots and self.snapshot_selection == "latest":
                        tenant_file_list = self._select_latest_per_partition(tenant_file_list)
                    if tenant_file_list:
                        pr = self._process_files(tenant_file_list, op_id, site_id=None)
                        all_failed.extend(pr.get("failed_paths", []))
                        total_expected += pr.get("expected", 0)
                else:
                    logger.info(
                        "Tenant-level entity snapshots are not yet available from the TCM API; "
                        "only site-level snapshots are supported at launch. Skipping tenant scope. "
                        "(Expected to become available in a future API release; use "
                        "--allow-tenant-snapshots to opt in when the API supports it.)"
                    )
        elif retrieve_tenant_logs:
            logger.info("Retrieving tenant logs...")
            tenant_file_list = self._get_file_list_for_event_types(
                op_id, window_start, window_end, event_types,
                site_id=None, max_chunk_hours=self.max_chunk_hours,
            )
            if tenant_file_list:
                pr = self._process_files(tenant_file_list, op_id, site_id=None)
                all_failed.extend(pr.get("failed_paths", []))
                total_expected += pr.get("expected", 0)
            else:
                logger.info("No tenant logs found.")

        # ---- Site scope ----
        label = "snapshots" if is_snapshots else "logs"

        if is_snapshots:
            # Collect all site files first (needed for cross-site latest-selection and gap check).
            # Tag each entry with its source site_id so we can route presigned-URL requests
            # back to the correct site-scoped endpoint after cross-site selection.
            all_site_files: List[dict] = []
            for s_id in self.site_ids:
                logger.info("Listing %s from site: %s", label, self._site_log_label(s_id))
                site_files = self._get_file_list_for_event_types(
                    op_id, window_start, window_end, event_types,
                    site_id=s_id, max_chunk_hours=self.max_chunk_hours,
                )
                for entry in site_files:
                    entry = dict(entry)
                    entry["_site_id"] = s_id
                    all_site_files.append(entry)
            # Delivery-gap check across all sites before selection.
            self._check_delivery_gaps(all_site_files, event_types)
            # Apply latest-per-partition selection for snapshot mode.
            if self.snapshot_selection == "latest":
                all_site_files = self._select_latest_per_partition(all_site_files)
            if all_site_files:
                # Group by source site and process each group with the correct site_id
                # so presigned-URL requests use the site-scoped endpoint, not the tenant endpoint.
                from collections import defaultdict
                files_by_site: dict = defaultdict(list)
                for entry in all_site_files:
                    files_by_site[entry.get("_site_id")].append(entry)

                selection_label = "latest-only" if self.snapshot_selection == "latest" else "all"
                logger.info(
                    "Snapshots: %s selection complete; %d file(s) across %d site(s).",
                    selection_label, len(all_site_files), len(files_by_site),
                )

                for s_id, site_entries in files_by_site.items():
                    site_label = self._site_log_label(s_id)
                    # Build per-entity-type counts for this site.
                    entity_counts: Dict[str, int] = {}
                    for entry in site_entries:
                        path = entry.get("path") or ""
                        ev = extract_segment_value_from_path(path, self.profile.path_key)
                        if ev:
                            entity_counts[ev] = entity_counts.get(ev, 0) + 1
                    sorted_entities = sorted(entity_counts.items(), key=lambda t: t[1], reverse=True)
                    top_n = 5
                    top_summary = ", ".join(f"{e}={c}" for e, c in sorted_entities[:top_n])
                    if len(sorted_entities) > top_n:
                        top_summary += f", ... ({len(sorted_entities)} entity types total)"
                    logger.info(
                        "Processing snapshots for site %s: %d file(s) [%s] -- %s",
                        site_label, len(site_entries), selection_label, top_summary,
                    )
                    if self.log_level == "debug":
                        full_summary = ", ".join(f"{e}={c}" for e, c in sorted_entities)
                        logger.debug(
                            "Full entity-type breakdown for site %s: %s",
                            site_label, full_summary,
                        )
                    pr = self._process_files(site_entries, op_id, site_id=s_id)
                    all_failed.extend(pr.get("failed_paths", []))
                    total_expected += pr.get("expected", 0)
            else:
                logger.info("No snapshots found in the lookback window.")
        else:
            # Activity-log: process per-site so the site_id is preserved in logs.
            for s_id in self.site_ids:
                logger.info("Retrieving %s from site: %s", label, self._site_log_label(s_id))
                site_file_list = self._get_file_list_for_event_types(
                    op_id, window_start, window_end, event_types,
                    site_id=s_id, max_chunk_hours=self.max_chunk_hours,
                )
                if site_file_list:
                    pr = self._process_files(site_file_list, op_id, site_id=s_id)
                    all_failed.extend(pr.get("failed_paths", []))
                    total_expected += pr.get("expected", 0)
                else:
                    logger.info("No %s found for site %s.", label, self._site_log_label(s_id))

        duration = int((time.time() - start_time_clock) * 1000)
        overall = "success" if not all_failed else "degraded"
        self._log(op_id, op_name, "complete", result={
            "status": overall,
            "dataset": self.profile.name,
            "expected_downloads": total_expected,
            "failed_path_count": len(all_failed),
            "failed_paths": all_failed,
        }, duration=duration)
        if all_failed:
            return ExtractionResult(
                status="degraded",
                failed_paths=sorted(set(all_failed)),
                expected_downloads=total_expected,
            )
        return ExtractionResult(status="success", failed_paths=[], expected_downloads=total_expected)

    def list_partition_values(self, filter_values=None, retrieve_tenant=False):
        """List unique partition values (eventType or entityType) from API metadata without downloading.

        Works for both ACTIVITYLOG (lists eventType values) and SNAPSHOTS (lists entityType values).
        The time window used is self.start_time/end_time; for SNAPSHOTS without an explicit window,
        the cadence-aware default window is used.

        :param filter_values: Optional list to restrict the API query (passed to the list call).
        :param retrieve_tenant: Whether to also query the tenant scope (activitylog only).
        :returns: Sorted list of partition values discovered, or None on login failure.
        """
        op_id = create_operation_id()
        op_name = "list_partition_values"
        start_time = time.time()
        discovered = set()
        is_snapshots = (self.profile is SNAPSHOTS or self.profile.name == "snapshots")

        if is_snapshots and not self.start_time and not self.end_time:
            window_start, window_end = self._cadence_default_window()
        else:
            window_start, window_end = self.start_time, self.end_time

        span_hours = compute_span_hours(window_start, window_end)
        chunks = generate_time_chunks(window_start, window_end, max_hours=self.max_chunk_hours)
        self._log(op_id, op_name, "start", inputs={
            "dataset": self.profile.name,
            "filter_values": filter_values,
            "retrieve_tenant": retrieve_tenant,
            "start_time": window_start,
            "end_time": window_end,
            "span_hours": round(span_hours, 2),
            "api_request_chunks": len(chunks),
        })
        logger.info(
            "Date range spans %.1f days (%.1f hours); will issue %d API chunk(s) of up to %d hours each.",
            span_hours / 24,
            span_hours,
            len(chunks),
            self.max_chunk_hours,
        )

        if not self.session_token and not self._login(op_id):
            duration = int((time.time() - start_time) * 1000)
            fail: dict = {"status": "failed", "details": "login failed"}
            fail.update(self._last_tcm_api_error)
            self._log(op_id, op_name, "complete", result=fail, duration=duration)
            if self._last_tcm_api_error:
                print_tcm_api_error_body(self._last_tcm_api_error)
            return None

        def _collect(file_list):
            for entry in file_list:
                pv = extract_segment_value_from_path(entry.get("path") or "", self.profile.path_key)
                if pv:
                    discovered.add(pv)

        if retrieve_tenant and not is_snapshots:
            logger.info("Listing tenant %s values...", self.profile.path_key)
            tenant_file_list = self._get_file_list_for_event_types(
                op_id, window_start, window_end, filter_values,
                site_id=None, max_chunk_hours=self.max_chunk_hours,
            )
            _collect(tenant_file_list)

        for s_id in self.site_ids:
            logger.info("Listing %s values from site: %s", self.profile.path_key, self._site_log_label(s_id))
            site_file_list = self._get_file_list_for_event_types(
                op_id, window_start, window_end, filter_values,
                site_id=s_id, max_chunk_hours=self.max_chunk_hours,
            )
            _collect(site_file_list)

        sorted_values = sorted(discovered)
        duration = int((time.time() - start_time) * 1000)
        self._log(op_id, op_name, "complete", result={
            "status": "success",
            "partition_key": self.profile.path_key,
            "value_count": len(sorted_values),
            "values": sorted_values,
        }, duration=duration)
        return sorted_values

    def list_event_types(self, event_types=None, retrieve_tenant_logs=False):
        """Back-compat alias for :meth:`list_partition_values` for activity-log callers."""
        return self.list_partition_values(filter_values=event_types, retrieve_tenant=retrieve_tenant_logs)

    def run_reference_pass(self, entities, max_results=100):
        """
        Pull tenant-wide reference data (tenant_sites, tenant_users) from the TCM
        REST API and write raw JSON pages to the extract storage backend.

        Must be called after a successful :meth:`_login` (self.tenant_id must be set).

        :param entities: Iterable of :class:`~common.reference_entity.ReferenceEntity`.
        :param max_results: Page size for each paginated GET (default 100).
        :returns: List of :class:`~extractor.reference.ReferenceResult`.
        :raises RuntimeError: If not authenticated (tenant_id not set).
        """
        if not self.tenant_id:
            raise RuntimeError(
                "run_reference_pass called before authentication. "
                "Call _login() first."
            )

        from extractor.reference import run_reference_pass

        results = []
        for entity in entities:
            op_id = create_operation_id()
            result = run_reference_pass(
                entity=entity,
                api_url=self.api_url,
                tenant_id=self.tenant_id,
                session=self.session,
                auth_headers_fn=lambda: self._auth_headers(),
                reauth_fn=lambda: self._reauth(create_operation_id()),
                storage_backend=self.storage_backend,
                op_id=op_id,
                max_results=max_results,
                wait_time=self.wait_time,
                base_backoff=self.base_backoff_seconds,
                max_backoff=self.max_backoff_cap,
                api_retry_max_total_wait_seconds=self.api_retry_max_total_wait_seconds,
            )
            results.append(result)
            logger.info(
                "Reference entity '%s': %d items across %d page(s) in %s.",
                entity.name, result.total_items, result.pages_written, result.output_dir,
            )
        return results
