"""
Tableau Server/Cloud publish: auth, site/project resolution, file upload, Publish Data Source, Update Data in Hyper.
"""

import json
import os
import uuid
from typing import Any, Dict, List, Optional, Tuple

import requests

DEFAULT_API_VERSION = "3.28"


class TableauPublishError(Exception):
    """Raised when a Tableau publish step fails, with context and remediation hint."""

    def __init__(
        self,
        step: str,
        message: str,
        inputs: Dict[str, Any],
        suggestion: str,
        cause: Optional[Exception] = None,
    ):
        self.step = step
        self.message = message
        self.inputs = inputs
        self.suggestion = suggestion
        self.cause = cause
        parts = [
            f"Tableau publish failed at step: {step}.",
            f"What went wrong: {message}",
            f"Inputs: {inputs}",
            f"Remedy: {suggestion}",
        ]
        super().__init__("\n".join(parts))


def _base_url(server_url: str) -> str:
    url = (server_url or "").rstrip("/")
    if not url:
        raise ValueError("server_url is required")
    return url


def sign_in(
    server_url: str,
    token_name: str,
    token_secret: str,
    site_content_url: str = "",
    api_version: str = DEFAULT_API_VERSION,
) -> Tuple[str, str]:
    """
    Sign in with Personal Access Token. Returns auth token and site LUID for subsequent requests.

    The Sign In response includes the site we signed in to; we use that site id and avoid
    calling the list-sites endpoint, which is not available on Tableau Cloud.

    :param server_url: Base URL (e.g. https://xxx.tableau.com).
    :param token_name: PAT name.
    :param token_secret: PAT secret.
    :param site_content_url: Site content URL (empty for default site).
    :param api_version: API version (e.g. 3.28).
    :return: (token for X-Tableau-Auth header, site LUID from response or empty string).
    """
    step = "sign-in"
    inputs_safe = {
        "server_url": server_url,
        "token_name": token_name,
        "site": site_content_url or "(default)",
    }
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": token_name,
            "personalAccessTokenSecret": token_secret,
            "site": {"contentUrl": site_content_url or ""},
        }
    }
    try:
        r = requests.post(
            url, json=payload, headers={"Accept": "application/json"}, timeout=60
        )
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        suggestion = (
            "Check that server_url is reachable, uses https, and has no fragment (#). "
            "Use the base URL only (e.g. https://10az.online.tableau.com) and set 'site' to the site content URL."
        )
        raise TableauPublishError(
            step,
            f"Request to Tableau sign-in endpoint failed: {e}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    try:
        data = r.json()
    except json.JSONDecodeError as e:
        preview = (r.text or "")[:200].strip()
        msg = (
            f"Server returned non-JSON (status {r.status_code}). "
            f"Response preview: {preview!r}"
        )
        if "#" in (server_url or ""):
            suggestion = (
                "server_url must not contain a fragment (#). Use only the base URL "
                "(e.g. https://10az.online.tableau.com) and set 'site' in your publish config "
                "to the site content URL (e.g. tableausalesdemo from #/site/tableausalesdemo)."
            )
        else:
            suggestion = (
                "Verify server_url points to Tableau Server/Cloud and the REST API is available. "
                "If using a proxy or VPN, ensure it allows API access."
            )
        raise TableauPublishError(
            step, msg, inputs_safe, suggestion, cause=e
        ) from e
    creds = data.get("credentials") or {}
    token = creds.get("token")
    if not token:
        suggestion = (
            "Check that your Personal Access Token (PAT) is valid, not expired, and has sign-in scope. "
            "Ensure 'site' matches the site content URL (e.g. tableausalesdemo) when not using the default site."
        )
        raise TableauPublishError(
            step,
            "Sign-in response did not contain an auth token.",
            inputs_safe,
            suggestion,
        )
    # Site id is returned in the sign-in response; use it so we don't need to call list-sites (unavailable on Tableau Cloud).
    site_id = (creds.get("site") or {}).get("id") or ""
    return (token, site_id)


def resolve_site_id(
    server_url: str,
    auth_token: str,
    site_name_or_luid: str,
    api_version: str = DEFAULT_API_VERSION,
) -> str:
    """
    Resolve site name or LUID to site LUID. If site_name_or_luid looks like a LUID (hex with dashes), return as-is.
    Otherwise query sites and match by name or contentUrl.

    :param server_url: Base URL.
    :param auth_token: X-Tableau-Auth token.
    :param site_name_or_luid: Site name, contentUrl, or LUID.
    :param api_version: API version.
    :return: Site LUID.
    """
    step = "resolve_site_id"
    inputs_safe = {"server_url": server_url, "site": site_name_or_luid or "(default)"}
    suggestion = (
        "Check that 'site' matches a site content URL or LUID on the server (e.g. tableausalesdemo)."
    )
    s = (site_name_or_luid or "").strip()
    if not s:
        return ""
    # Heuristic: LUID is typically UUID-like
    if len(s) >= 30 and "-" in s and all(c in "0123456789abcdefABCDEF-" for c in s):
        return s
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites"
    try:
        r = requests.get(
            url,
            headers={"X-Tableau-Auth": auth_token, "Accept": "application/json"},
            params={"pageSize": 1000},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(
            step,
            f"Request to list sites failed: {e}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    except json.JSONDecodeError as e:
        raise TableauPublishError(
            step,
            f"Server returned non-JSON (status {r.status_code}). Response preview: {(r.text or '')[:200]!r}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    sites = (data.get("sites") or {}).get("site") or []
    if isinstance(sites, dict):
        sites = [sites]
    for site in sites:
        if site.get("id") == s or site.get("name") == s or site.get("contentUrl") == s:
            return site.get("id") or s
    if sites and not s:
        return sites[0].get("id") or ""
    raise TableauPublishError(
        step,
        f"Site not found: {site_name_or_luid}",
        inputs_safe,
        suggestion,
    )


def _create_project(
    server_url: str,
    auth_token: str,
    site_id: str,
    name: str,
    parent_project_id: Optional[str] = None,
    api_version: str = DEFAULT_API_VERSION,
) -> Dict[str, Any]:
    """Create a project under the optional parent project."""
    step = "create_project"
    inputs_safe = {"server_url": server_url, "site_id": site_id, "name": name, "parent": parent_project_id or "(root)"}
    suggestion = "Check that the token has project create permissions on the site."
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites/{site_id}/projects"
    payload: Dict[str, Any] = {"project": {"name": name}}
    if parent_project_id:
        payload["project"]["parentProjectId"] = parent_project_id
    try:
        r = requests.post(
            url,
            headers={"X-Tableau-Auth": auth_token, "Content-Type": "application/json", "Accept": "application/json"},
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(step, f"Failed to create project '{name}': {e}", inputs_safe, suggestion, cause=e) from e
    except json.JSONDecodeError as e:
        raise TableauPublishError(
            step,
            f"Server returned non-JSON (status {r.status_code}). Response preview: {(r.text or '')[:200]!r}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    project = data.get("project") or {}
    if not project.get("id"):
        raise TableauPublishError(step, f"Create project response missing 'id': {data}", inputs_safe, suggestion)
    return project


def resolve_project_id(
    server_url: str,
    auth_token: str,
    site_id: str,
    project_path_or_name_or_luid: str,
    api_version: str = DEFAULT_API_VERSION,
    create_missing: bool = False,
) -> str:
    """
    Resolve project by "/" path, name, or LUID. Path is parsed and each segment resolved by name (parentProjectId).
    If single name and multiple projects match, error. When create_missing=True, missing path segments are created.

    :param server_url: Base URL.
    :param auth_token: Auth token.
    :param site_id: Site LUID.
    :param project_path_or_name_or_luid: e.g. "Analytics/Logs", "MyProject", or LUID.
    :param api_version: API version.
    :param create_missing: If True, create missing project path segments instead of raising.
    :return: Project LUID.
    """
    step = "resolve_project_id"
    inputs_safe = {
        "server_url": server_url,
        "site_id": site_id,
        "project": project_path_or_name_or_luid or "(empty)",
    }
    suggestion = (
        "Check that 'project' matches a project name, path (e.g. Z_Admin Insights/PDA), or project LUID."
    )
    s = (project_path_or_name_or_luid or "").strip()
    if not s:
        raise TableauPublishError(
            step, "project is required", inputs_safe, suggestion
        )
    # LUID-like
    if len(s) >= 30 and "-" in s and all(c in "0123456789abcdefABCDEF-" for c in s):
        return s
    base = _base_url(server_url)
    segments = [p.strip() for p in s.split("/") if p.strip()]
    if not segments:
        raise TableauPublishError(
            step, "project path or name is empty", inputs_safe, suggestion
        )
    # Fetch all projects for site
    url = f"{base}/api/{api_version}/sites/{site_id}/projects"
    try:
        r = requests.get(
            url,
            headers={"X-Tableau-Auth": auth_token, "Accept": "application/json"},
            params={"pageSize": 1000},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(
            step,
            f"Request to list projects failed: {e}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    except json.JSONDecodeError as e:
        raise TableauPublishError(
            step,
            f"Server returned non-JSON (status {r.status_code}). Response preview: {(r.text or '')[:200]!r}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    all_projects = (data.get("projects") or {}).get("project") or []
    if isinstance(all_projects, dict):
        all_projects = [all_projects]
    by_id = {p.get("id"): p for p in all_projects if p.get("id")}
    by_parent = {}
    for p in all_projects:
        parent = p.get("parentProjectId") or ""
        by_parent.setdefault(parent, []).append(p)
    if len(segments) == 1:
        name = segments[0]
        matches = [p for p in all_projects if (p.get("name") or "") == name]
        if len(matches) > 1:
            raise TableauPublishError(
                step,
                f"Multiple projects named '{name}'; use full path or LUID",
                inputs_safe,
                suggestion,
            )
        if len(matches) == 0:
            if create_missing:
                created = _create_project(server_url, auth_token, site_id, name, api_version=api_version)
                return created.get("id")
            raise TableauPublishError(
                step, f"Project not found: {name}", inputs_safe, suggestion
            )
        return matches[0].get("id")
    # Walk path
    parent_id = ""
    for name in segments:
        children = by_parent.get(parent_id) or []
        matches = [p for p in children if (p.get("name") or "") == name]
        if len(matches) > 1:
            raise TableauPublishError(
                step,
                f"Multiple projects named '{name}' under path; use LUID",
                inputs_safe,
                suggestion,
            )
        if len(matches) == 0:
            if create_missing:
                created = _create_project(
                    server_url, auth_token, site_id, name,
                    parent_project_id=parent_id or None,
                    api_version=api_version,
                )
                parent_id = created.get("id")
                by_parent.setdefault(parent_id, [])
                continue
            raise TableauPublishError(
                step,
                f"Project not found in path: {name}",
                inputs_safe,
                suggestion,
            )
        parent_id = matches[0].get("id")
    return parent_id


def initiate_file_upload(
    server_url: str,
    auth_token: str,
    site_id: str,
    api_version: str = DEFAULT_API_VERSION,
) -> str:
    """
    Initiate a file upload session. Returns upload session ID.

    :param server_url: Base URL.
    :param auth_token: Auth token.
    :param site_id: Site LUID.
    :param api_version: API version.
    :return: uploadSessionId.
    """
    step = "initiate_file_upload"
    inputs_safe = {"server_url": server_url, "site_id": site_id}
    suggestion = "Ensure the auth token and site are valid; retry after a short delay."
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites/{site_id}/fileUploads"
    try:
        r = requests.post(
            url,
            headers={"X-Tableau-Auth": auth_token, "Accept": "application/json"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(
            step, f"Request to initiate file upload failed: {e}", inputs_safe, suggestion, cause=e
        ) from e
    except json.JSONDecodeError as e:
        raise TableauPublishError(
            step,
            f"Server returned non-JSON (status {r.status_code}). Response preview: {(r.text or '')[:200]!r}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    file_upload = (data.get("fileUpload") or {})
    upload_id = file_upload.get("uploadSessionId")
    if not upload_id:
        raise TableauPublishError(
            step,
            "Initiate file upload response missing uploadSessionId",
            inputs_safe,
            suggestion,
        )
    return upload_id


def append_to_file_upload(
    server_url: str,
    auth_token: str,
    site_id: str,
    upload_session_id: str,
    file_path: str,
    api_version: str = DEFAULT_API_VERSION,
) -> None:
    """
    Append file contents to an upload session using MIME multipart format
    required by the Tableau REST API (request_payload + tableau_file parts).

    :param server_url: Base URL.
    :param auth_token: Auth token.
    :param site_id: Site LUID.
    :param upload_session_id: From initiate_file_upload.
    :param file_path: Local path to file.
    :param api_version: API version.
    """
    step = "append_to_file_upload"
    inputs_safe = {"server_url": server_url, "site_id": site_id, "file_path": file_path}
    suggestion = "Ensure the Hyper file exists and is not locked; check network stability."
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites/{site_id}/fileUploads/{upload_session_id}"
    try:
        with open(file_path, "rb") as f:
            file_bytes = f.read()
    except OSError as e:
        raise TableauPublishError(
            step, f"Could not read file: {e}", inputs_safe, suggestion, cause=e
        ) from e
    filename = os.path.basename(file_path)
    boundary = "----boundary_" + uuid.uuid4().hex[:16]
    # Tableau expects MIME multipart: empty request_payload part, then tableau_file part.
    # Blank payload must have two CRLFs for RFC compliance.
    body = (
        f"--{boundary}\r\n"
        "Content-Disposition: name=\"request_payload\"\r\n"
        "Content-Type: text/xml\r\n\r\n\r\n"
        f"--{boundary}\r\n"
        f"Content-Disposition: name=\"tableau_file\"; filename=\"{filename}\"\r\n"
        "Content-Type: application/octet-stream\r\n\r\n"
    ).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")
    try:
        r = requests.put(
            url,
            headers={
                "X-Tableau-Auth": auth_token,
                "Content-Type": f"multipart/mixed; boundary={boundary}",
                "Accept": "application/json",
            },
            data=body,
            timeout=300,
        )
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(
            step, f"Request to upload file failed: {e}", inputs_safe, suggestion, cause=e
        ) from e


def publish_datasource(
    server_url: str,
    auth_token: str,
    site_id: str,
    project_id: str,
    datasource_name: str,
    upload_session_id: str,
    file_type: str = "hyper",
    overwrite: bool = False,
    description: Optional[str] = None,
    api_version: str = DEFAULT_API_VERSION,
) -> Dict[str, Any]:
    """
    Commit an uploaded file as a new (or overwritten) data source.
    Uses multipart request with request_payload part (Tableau requirement).

    :param server_url: Base URL.
    :param auth_token: Auth token.
    :param site_id: Site LUID.
    :param project_id: Project LUID.
    :param datasource_name: Name for the data source.
    :param upload_session_id: From initiate_file_upload + append_to_file_upload.
    :param file_type: hyper, tds, tdsx, or tde.
    :param overwrite: Whether to overwrite existing same-name data source.
    :param description: Optional description for the data source.
    :param api_version: API version.
    :return: Response dict with datasource id etc.
    """
    import json as _json
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites/{site_id}/datasources"
    params = {
        "uploadSessionId": upload_session_id,
        "datasourceType": file_type,
        "overwrite": str(overwrite).lower(),
    }
    datasource_payload = {
        "name": datasource_name,
        "project": {"id": project_id},
    }
    if description is not None and description != "":
        datasource_payload["description"] = description
    payload = {"datasource": datasource_payload}
    boundary = "----boundary_" + uuid.uuid4().hex[:16]
    body = (
        f"--{boundary}\r\n"
        "Content-Disposition: name=\"request_payload\"\r\n"
        "Content-Type: application/json\r\n\r\n"
        f"{_json.dumps(payload)}\r\n"
        f"--{boundary}--\r\n"
    )
    step = "publish_datasource"
    inputs_safe = {
        "server_url": server_url,
        "site_id": site_id,
        "project_id": project_id,
        "datasource_name": datasource_name,
    }
    suggestion = (
        "Check that the project exists, you have publish permission, and that overwrite is set as intended if the datasource name already exists."
    )
    try:
        r = requests.post(
            url,
            params=params,
            headers={
                "X-Tableau-Auth": auth_token,
                "Content-Type": f"multipart/mixed; boundary={boundary}",
                "Accept": "application/json",
            },
            data=body.encode("utf-8"),
            timeout=120,
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(
            step, f"Request to publish datasource failed: {e}", inputs_safe, suggestion, cause=e
        ) from e
    except json.JSONDecodeError as e:
        raise TableauPublishError(
            step,
            f"Server returned non-JSON (status {r.status_code}). Response preview: {(r.text or '')[:200]!r}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e


def query_datasources(
    server_url: str,
    auth_token: str,
    site_id: str,
    name: Optional[str] = None,
    api_version: str = DEFAULT_API_VERSION,
) -> List[Dict[str, Any]]:
    """
    Query data sources on the site, optionally filtered by name.

    The Tableau REST API supports 'name' as a filter field for datasources.
    The filter is appended directly to the URL (not via params dict) to prevent
    requests from percent-encoding the colons in the filter syntax, which causes a 400.
    projectId is not a supported filter field; callers should filter by project client-side.

    :param name: Optional datasource name to filter by server-side (exact match).
    :return: List of datasource dicts (id, name, project, etc.).
    """
    import urllib.parse
    step = "query_datasources"
    inputs_safe = {"server_url": server_url, "site_id": site_id, "name": name or "(all)"}
    suggestion = "Check that the auth token is valid and has datasource read permissions on the site."
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites/{site_id}/datasources?pageSize=1000"
    if name:
        url += f"&filter=name:eq:{urllib.parse.quote(name, safe='')}"
    try:
        r = requests.get(
            url,
            headers={"X-Tableau-Auth": auth_token, "Accept": "application/json"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(
            step, f"Request to query datasources failed: {e}", inputs_safe, suggestion, cause=e
        ) from e
    except json.JSONDecodeError as e:
        raise TableauPublishError(
            step,
            f"Server returned non-JSON (status {r.status_code}). Response preview: {(r.text or '')[:200]!r}",
            inputs_safe,
            suggestion,
            cause=e,
        ) from e
    ds_list = (data.get("datasources") or {}).get("datasource") or []
    if isinstance(ds_list, dict):
        ds_list = [ds_list]
    return ds_list


def update_hyper_data(
    server_url: str,
    auth_token: str,
    site_id: str,
    datasource_id: str,
    upload_session_id: str,
    actions: List[Dict[str, Any]],
    api_version: str = DEFAULT_API_VERSION,
) -> Dict[str, Any]:
    """
    PATCH Update Data in Hyper Data Source (incremental update with payload).

    The PATCH request is asynchronous: the server returns a job object immediately.
    Callers should use wait_for_job to poll until the job reaches a terminal state.

    :param server_url: Base URL.
    :param auth_token: Auth token.
    :param site_id: Site LUID.
    :param datasource_id: Data source LUID.
    :param upload_session_id: From initiate + append (payload Hyper file).
    :param actions: List of action dicts (action, source-table, target-table, condition if needed).
    :param api_version: API version.
    :return: Response dict containing a 'job' key with 'id' and initial status.
    """
    step = "hyper_update"
    inputs_safe = {
        "server_url": server_url,
        "site_id": site_id,
        "datasource_id": datasource_id,
    }
    suggestion = (
        "Check sign-in, site, project, and that the datasource exists on the server with a "
        "live-to-Hyper connection. Ensure the PAT has the tableau:hyper_data:update scope (API 3.27+)."
    )
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites/{site_id}/datasources/{datasource_id}/data"
    params = {"uploadSessionId": upload_session_id}
    payload = {"actions": actions}
    request_id = str(uuid.uuid4())
    try:
        r = requests.patch(
            url,
            params=params,
            headers={
                "X-Tableau-Auth": auth_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "RequestID": request_id,
            },
            json=payload,
            timeout=60,
        )
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise TableauPublishError(
            step, f"Request to update Hyper data failed: {e}", inputs_safe, suggestion, cause=e
        ) from e


def wait_for_job(
    server_url: str,
    auth_token: str,
    site_id: str,
    job_id: str,
    poll_interval: int = 5,
    timeout: int = 300,
    api_version: str = DEFAULT_API_VERSION,
) -> Tuple[bool, str]:
    """
    Poll GET /api/{version}/sites/{site_id}/jobs/{job_id} until the job reaches a terminal state
    or the timeout is exceeded.

    finishCode meanings (Tableau REST API): 0 = success, 1 = error, 2 = cancelled.
    A missing or -1 finishCode means the job is still in progress.

    :param poll_interval: Seconds between polls.
    :param timeout: Maximum total seconds to wait before giving up.
    :return: (success, notes) where notes is a human-readable status string for logging.
    """
    import time
    base = _base_url(server_url)
    url = f"{base}/api/{api_version}/sites/{site_id}/jobs/{job_id}"
    headers = {"X-Tableau-Auth": auth_token, "Accept": "application/json"}
    elapsed = 0
    while elapsed < timeout:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
        except requests.exceptions.RequestException:
            # Transient network error; keep polling
            time.sleep(poll_interval)
            elapsed += poll_interval
            continue
        job = data.get("job") or {}
        finish_code = job.get("finishCode")
        if finish_code is None or finish_code == -1:
            time.sleep(poll_interval)
            elapsed += poll_interval
            continue
        notes_raw = (job.get("statusNotes") or {}).get("statusNote") or []
        if isinstance(notes_raw, dict):
            notes_raw = [notes_raw]
        notes = "; ".join(n.get("text", "") for n in notes_raw if n.get("text"))
        if finish_code == 0:
            return (True, notes or "")
        elif finish_code == 2:
            return (False, notes or "job was cancelled")
        else:
            return (False, notes or f"job failed (finishCode={finish_code})")
    return (False, f"job did not complete within {timeout}s timeout")


def publish_hyper_to_tableau(
    server_url: str,
    auth_token: str,
    site_id: str,
    project_id: str,
    hyper_path: str,
    datasource_name: str,
    overwrite: bool = False,
    tds_path: Optional[str] = None,
    description: Optional[str] = None,
    api_version: str = DEFAULT_API_VERSION,
) -> Dict[str, Any]:
    """
    Publish a local Hyper file to Tableau (initiate upload, append file, publish).
    If tds_path is provided, build a .tdsx from the TDS and hyper, then publish the .tdsx.
    Otherwise publish the .hyper file directly as the data source.

    :param tds_path: Optional path to .tds file; if provided, a .tdsx is built and published.
    :param description: Optional description for the published data source.
    :return: Publish response.
    """
    tdsx_path = None
    try:
        if tds_path and os.path.isfile(tds_path):
            from loader.targets import tdsx_build
            tdsx_path = tdsx_build.build_tdsx(tds_path, hyper_path)
            file_to_upload = tdsx_path
            file_type = "tdsx"
        else:
            file_to_upload = hyper_path
            file_type = "hyper"
        upload_id = initiate_file_upload(server_url, auth_token, site_id, api_version)
        append_to_file_upload(server_url, auth_token, site_id, upload_id, file_to_upload, api_version)
        return publish_datasource(
            server_url,
            auth_token,
            site_id,
            project_id,
            datasource_name,
            upload_id,
            file_type=file_type,
            overwrite=overwrite,
            description=description,
            api_version=api_version,
        )
    finally:
        if tdsx_path and os.path.isfile(tdsx_path):
            try:
                os.remove(tdsx_path)
            except OSError:
                pass
