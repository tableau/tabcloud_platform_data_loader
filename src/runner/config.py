"""
Parse and model run.yml — the single orchestration entry point for tabcloud-run.

Schema top-level blocks:
  connection   (required) — TCM authentication.
  extraction   (optional) — named-job map; absent ⇒ synthesize 3 default jobs.
  transform    (optional) — named map of transformer mapping refs.
  load         (optional) — named map of loaders.
  destinations (optional) — named map of publish targets.
  storage      (optional) — local/S3 artifact bus.
  logging      (optional)

Block presence rule:
  • absent              → engine synthesizes the documented defaults.
  • present with entries → exactly what you wrote; you stop inheriting future
                           additions to the default set.
  • present but empty    → nothing; runner warns and treats as absent.

Scheduling is external: this file defines WHAT runs (one pipeline, once per
invocation). Use cron/launchd/Airflow/etc. to invoke `tabcloud-run -c run.yml`
on a cadence. For different cadences, use separate run-files.

Multi-tenant note: `connection:` is singular now (one TCM tenant per run-file).
Future work will promote this to an optional `connections:` named map with a
per-job `connection:` field for cross-tenant consolidation. The singular form
will remain valid as a shorthand for the single-connection default.
"""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml as _yaml
except ImportError:  # pragma: no cover
    _yaml = None  # type: ignore


# ---------------------------------------------------------------------------
# Data-type constants for ExtractionJobConfig.data
# ---------------------------------------------------------------------------

DATA_EVENT_LOGS = "event_logs"
DATA_SNAPSHOTS = "snapshots"
DATA_REFERENCE = "reference"
VALID_DATA_TYPES = frozenset({DATA_EVENT_LOGS, DATA_SNAPSHOTS, DATA_REFERENCE})

# ---------------------------------------------------------------------------
# Destination mode constants
# ---------------------------------------------------------------------------

DEST_MODE_UPSERT = "upsert"
DEST_MODE_HYPER_UPDATE = "hyper_update"
DEST_MODE_OVERWRITE = "overwrite"
VALID_DEST_MODES = frozenset({DEST_MODE_UPSERT, DEST_MODE_HYPER_UPDATE, DEST_MODE_OVERWRITE})

# ---------------------------------------------------------------------------
# Storage backend constants
# ---------------------------------------------------------------------------

BACKEND_LOCAL = "local"
BACKEND_S3 = "s3"
VALID_BACKENDS = frozenset({BACKEND_LOCAL, BACKEND_S3})

# ---------------------------------------------------------------------------
# Bundled mapping paths — resolved against the installed package root
# ---------------------------------------------------------------------------

def bundled_mappings_root() -> str:
    """
    Return the project root directory that contains the bundled ``mappings/``
    tree.  This file lives at ``<root>/src/runner/config.py``, so the root is
    two ``parents`` up.
    """
    return str(Path(__file__).resolve().parents[2])


def _bundled_mapping(rel: str) -> str:
    """Return an absolute path for a mapping file bundled with the package."""
    return os.path.join(bundled_mappings_root(), rel)


_DEFAULT_TRANSFORM_MAPPINGS = [
    "mappings/transformer/generic_activity_log_auto.yaml",
    "mappings/transformer/entity_snapshot_auto.yaml",
    "mappings/transformer/tenant_reference_auto.yaml",
]
_DEFAULT_LOADER_MAPPING = "mappings/loader/platform_data_bundle.yml"

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


@dataclass
class ConnectionConfig:
    """TCM authentication. Required."""
    tcm_url: str
    token_name: str
    token_secret: str  # always a secret_ref string


@dataclass
class TuningConfig:
    """Optional extractor performance/retry knobs. Sane defaults; rarely touched."""
    wait_time_seconds: int = 60
    page_size: int = 1000
    download_threads: int = 10
    presigned_url_batch_size: int = 100
    presigned_url_timeout_seconds: int = 30
    max_chunk_hours: int = 168
    overwrite: bool = False
    api_retry_max_total_wait_seconds: int = 300
    post_timeout_max_total_wait_seconds: int = 120
    download_retry_attempts: int = 3


@dataclass
class ExtractionJobConfig:
    """
    One named extraction job. The job name is an arbitrary label you choose;
    ``data`` is the selector that determines what gets pulled.
    """
    name: str
    data: str  # one of VALID_DATA_TYPES

    # ---- event_logs ----
    scope: str = "site"  # "site" | "tenant"
    sites: Any = "all"   # "all" or list[str] of contentUrl values
    event_types: List[str] = field(default_factory=list)
    # Time window (event_logs); applied as lookback or fixed window.
    # lookback_days used when start_time/end_time are both empty.
    lookback_days: int = 2
    start_time: str = ""
    end_time: str = ""

    # ---- snapshots ----
    entity_types: List[str] = field(default_factory=list)
    selection: str = "latest"      # "latest" | "all"
    cadence: str = "hourly"        # "hourly" | "daily"
    lookback_intervals: int = 4
    gap_action: str = "warn"       # "warn" | "error" | "ignore"

    # ---- reference ----
    entities: List[str] = field(default_factory=lambda: ["tenant_sites", "tenant_users"])

    # ---- shared ----
    folder_layout: str = ""   # "" | "date-first" | "schema-first"
    tuning: TuningConfig = field(default_factory=TuningConfig)


@dataclass
class TransformJobConfig:
    """One named transformer. The job name is an arbitrary label."""
    name: str
    mapping: str  # resolved absolute path to the transformer mapping YAML


@dataclass
class LoadJobConfig:
    """One named loader. The job name is an arbitrary label."""
    name: str
    mapping: str                   # resolved absolute path to the loader mapping YAML
    destination: Optional[str] = None  # name from destinations{}; None = local .hyper only
    datasource_name: str = ""      # published data source name (only used when publishing)
    description: str = ""          # published data source description


@dataclass
class DestinationConfig:
    """
    A named publish/export target. A loader publishes ONLY when it names one
    of these (via load.<name>.destination). No destination named ⇒ local only.
    """
    name: str
    type: str = "tableau"          # only "tableau" today
    server_url: str = ""           # Tableau Cloud/Server POD base URL (NOT tcm_url)
    token_name: str = ""
    token_secret: str = ""         # secret_ref
    site: str = ""                 # Tableau site contentUrl/LUID; "" = default site
    project: str = ""
    mode: str = DEST_MODE_UPSERT


@dataclass
class S3CredentialsConfig:
    provider: str = "aws_default_chain"
    # provider == static
    access_key: str = ""           # secret_ref
    secret_key: str = ""           # secret_ref
    # provider == aws_profile
    profile: str = ""
    # provider == aws_secrets_manager
    secrets_manager_id: str = ""
    secrets_manager_key_map: Dict[str, str] = field(default_factory=lambda: {
        "access_key": "AWS_ACCESS_KEY_ID",
        "secret_key": "AWS_SECRET_ACCESS_KEY",
    })


@dataclass
class S3StorageConfig:
    bucket: str = ""
    prefix: str = "tabcloud/"
    region: str = ""
    endpoint_url: str = ""
    credentials: S3CredentialsConfig = field(default_factory=S3CredentialsConfig)


@dataclass
class StorageConfig:
    backend: str = BACKEND_LOCAL   # "local" | "s3"
    path: str = "./data_workspace"  # local artifact root (resolved absolute)
    s3: S3StorageConfig = field(default_factory=S3StorageConfig)


@dataclass
class LoggingConfig:
    level: str = "INFO"            # DEBUG | INFO | WARNING | ERROR
    dir: str = ""                  # "" = {storage.path}/logs
    retention_days: int = 60


# ---------------------------------------------------------------------------
# Top-level RunConfig
# ---------------------------------------------------------------------------


@dataclass
class RunConfig:
    """
    Fully-resolved configuration for one tabcloud-run pipeline invocation.

    The three phase blocks (extraction, transform, load) have been expanded
    and all relative paths have been resolved to absolute paths.
    """
    version: int
    base_dir: str          # directory containing run.yml; paths resolved from here
    connection: ConnectionConfig
    extraction: Dict[str, ExtractionJobConfig]
    transform: Dict[str, TransformJobConfig]
    load: Dict[str, LoadJobConfig]
    destinations: Dict[str, DestinationConfig]
    storage: StorageConfig
    logging_cfg: LoggingConfig

    # True when the block was absent (synthesized defaults), vs present-with-entries.
    extraction_defaulted: bool = False
    transform_defaulted: bool = False
    load_defaulted: bool = False


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


class RunConfigError(ValueError):
    """Raised when run.yml cannot be parsed into a valid RunConfig."""


def load_run_config(config_path: str) -> RunConfig:
    """
    Parse a run.yml file into a RunConfig.

    :param config_path: Absolute path to the run.yml file.
    :raises RunConfigError: if the file cannot be parsed or a required field is missing.
    :raises FileNotFoundError: if the file does not exist.
    """
    if _yaml is None:  # pragma: no cover
        raise ImportError(
            "PyYAML is required to parse run.yml. Install with: pip install pyyaml"
        )
    config_path = os.path.abspath(config_path)
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"run.yml not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        raw = _yaml.safe_load(f) or {}

    base_dir = os.path.dirname(config_path)

    def _resolve(p: str) -> str:
        """Resolve a path relative to base_dir; leave absolute paths alone."""
        if not p:
            return p
        return p if os.path.isabs(p) else os.path.abspath(os.path.join(base_dir, p))

    version = int(raw.get("version", 1))

    # -- connection (required) --
    conn_raw = raw.get("connection") or {}
    if not conn_raw:
        raise RunConfigError(
            "run.yml is missing the required `connection:` block. "
            "Add your Tableau Cloud Manager URL and credentials."
        )
    connection = ConnectionConfig(
        tcm_url=(conn_raw.get("tcm_url") or "").strip(),
        token_name=(conn_raw.get("token_name") or "").strip(),
        token_secret=(conn_raw.get("token_secret") or "").strip(),
    )
    if not connection.tcm_url:
        raise RunConfigError("connection.tcm_url is required.")
    if not connection.token_name:
        raise RunConfigError("connection.token_name is required.")
    if not connection.token_secret:
        raise RunConfigError("connection.token_secret is required.")

    # -- storage (optional) --
    storage = _parse_storage(raw.get("storage"), base_dir, _resolve)

    # -- logging (optional) --
    logging_cfg = _parse_logging(raw.get("logging"))

    # -- destinations (optional) --
    destinations = _parse_destinations(raw.get("destinations"))

    # -- extraction (optional) --
    extraction_raw = raw.get("extraction")
    extraction_defaulted = extraction_raw is None
    if extraction_defaulted:
        extraction = _default_extraction_jobs()
    elif not extraction_raw:
        # Present but empty — warn; treat as absent.
        import warnings
        warnings.warn(
            "run.yml has an empty `extraction:` block. "
            "Defaulting to the standard 3-job extraction. "
            "Remove the block or add at least one job.",
            stacklevel=2,
        )
        extraction = _default_extraction_jobs()
        extraction_defaulted = True
    else:
        extraction = {
            name: _parse_extraction_job(name, job_raw)
            for name, job_raw in extraction_raw.items()
        }

    # -- transform (optional) --
    transform_raw = raw.get("transform")
    transform_defaulted = transform_raw is None
    if transform_defaulted:
        transform = _default_transform_jobs(base_dir, _resolve)
    elif not transform_raw:
        import warnings
        warnings.warn(
            "run.yml has an empty `transform:` block. "
            "Defaulting to the standard auto-transformer set.",
            stacklevel=2,
        )
        transform = _default_transform_jobs(base_dir, _resolve)
        transform_defaulted = True
    else:
        transform = {
            name: TransformJobConfig(
                name=name,
                mapping=_resolve((t.get("mapping") or "").strip()),
            )
            for name, t in transform_raw.items()
        }

    # -- load (optional) --
    load_raw = raw.get("load")
    load_defaulted = load_raw is None
    if load_defaulted:
        load = _default_load_jobs(base_dir, _resolve)
    elif not load_raw:
        import warnings
        warnings.warn(
            "run.yml has an empty `load:` block. "
            "Defaulting to the standard local .hyper loader.",
            stacklevel=2,
        )
        load = _default_load_jobs(base_dir, _resolve)
        load_defaulted = True
    else:
        load = {
            name: _parse_load_job(name, lj, _resolve)
            for name, lj in load_raw.items()
        }

    return RunConfig(
        version=version,
        base_dir=base_dir,
        connection=connection,
        extraction=extraction,
        transform=transform,
        load=load,
        destinations=destinations,
        storage=storage,
        logging_cfg=logging_cfg,
        extraction_defaulted=extraction_defaulted,
        transform_defaulted=transform_defaulted,
        load_defaulted=load_defaulted,
    )


# ---------------------------------------------------------------------------
# Section parsers
# ---------------------------------------------------------------------------


def _parse_storage(raw: Any, base_dir: str, _resolve) -> StorageConfig:
    if not raw:
        return StorageConfig(path=_resolve("./data_workspace"))
    backend = (raw.get("backend") or BACKEND_LOCAL).strip().lower()
    path_raw = (raw.get("path") or "./data_workspace").strip()
    path = path_raw if os.path.isabs(path_raw) else os.path.abspath(
        os.path.join(base_dir, path_raw)
    )
    s3_cfg = S3StorageConfig()
    if backend == BACKEND_S3:
        s3_raw = raw.get("s3") or {}
        creds_raw = s3_raw.get("credentials") or {}
        s3_cfg = S3StorageConfig(
            bucket=(s3_raw.get("bucket") or "").strip(),
            prefix=(s3_raw.get("prefix") or "tabcloud/").strip(),
            region=(s3_raw.get("region") or "").strip(),
            endpoint_url=(s3_raw.get("endpoint_url") or "").strip(),
            credentials=S3CredentialsConfig(
                provider=(creds_raw.get("provider") or "aws_default_chain").strip(),
                access_key=(creds_raw.get("access_key") or "").strip(),
                secret_key=(creds_raw.get("secret_key") or "").strip(),
                profile=(creds_raw.get("profile") or "").strip(),
                secrets_manager_id=(
                    (creds_raw.get("secrets_manager") or {}).get("secret_id") or ""
                ).strip(),
                secrets_manager_key_map=(
                    (creds_raw.get("secrets_manager") or {}).get("key_map") or {
                        "access_key": "AWS_ACCESS_KEY_ID",
                        "secret_key": "AWS_SECRET_ACCESS_KEY",
                    }
                ),
            ),
        )
    return StorageConfig(backend=backend, path=path, s3=s3_cfg)


def _parse_logging(raw: Any) -> LoggingConfig:
    if not raw:
        return LoggingConfig()
    return LoggingConfig(
        level=(raw.get("level") or "INFO").strip().upper(),
        dir=(raw.get("dir") or "").strip(),
        retention_days=int(raw.get("retention_days") or 60),
    )


def _parse_destinations(raw: Any) -> Dict[str, DestinationConfig]:
    if not raw:
        return {}
    result = {}
    for name, d in raw.items():
        if not d:
            continue
        result[name] = DestinationConfig(
            name=name,
            type=(d.get("type") or "tableau").strip().lower(),
            server_url=(d.get("server_url") or "").strip(),
            token_name=(d.get("token_name") or "").strip(),
            token_secret=(d.get("token_secret") or "").strip(),
            site=(d.get("site") or "").strip(),
            project=(d.get("project") or "").strip(),
            mode=(d.get("mode") or DEST_MODE_UPSERT).strip().lower(),
        )
    return result


def _parse_extraction_job(name: str, raw: Any) -> ExtractionJobConfig:
    raw = raw or {}
    data = (raw.get("data") or "").strip().lower()
    tuning_raw = raw.get("tuning") or {}
    tuning = TuningConfig(
        wait_time_seconds=int(tuning_raw.get("wait_time_seconds", 60)),
        page_size=int(tuning_raw.get("page_size", 1000)),
        download_threads=int(tuning_raw.get("download_threads", 10)),
        presigned_url_batch_size=int(tuning_raw.get("presigned_url_batch_size", 100)),
        presigned_url_timeout_seconds=int(tuning_raw.get("presigned_url_timeout_seconds", 30)),
        max_chunk_hours=int(tuning_raw.get("max_chunk_hours", 168)),
        overwrite=bool(tuning_raw.get("overwrite", False)),
        api_retry_max_total_wait_seconds=int(tuning_raw.get("api_retry_max_total_wait_seconds", 300)),
        post_timeout_max_total_wait_seconds=int(tuning_raw.get("post_timeout_max_total_wait_seconds", 120)),
        download_retry_attempts=int(tuning_raw.get("download_retry_attempts", 3)),
    )
    # sites: "all" string or list
    sites_raw = raw.get("sites", "all")
    if isinstance(sites_raw, list):
        sites: Any = [str(s).strip() for s in sites_raw]
    else:
        sites = (str(sites_raw) or "all").strip().lower()
        if sites not in ("all", "none"):
            # treat any other single string as a comma-separated list
            sites = [s.strip() for s in sites.split(",") if s.strip()]
    # event_types / entity_types
    def _as_list(v: Any) -> List[str]:
        if not v:
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if x]
        return [s.strip() for s in str(v).split(",") if s.strip()]
    return ExtractionJobConfig(
        name=name,
        data=data,
        scope=(raw.get("scope") or "site").strip().lower(),
        sites=sites,
        event_types=_as_list(raw.get("event_types")),
        lookback_days=int(raw.get("lookback_days", 2)),
        start_time=(raw.get("start_time") or "").strip(),
        end_time=(raw.get("end_time") or "").strip(),
        entity_types=_as_list(raw.get("entity_types")),
        selection=(raw.get("selection") or "latest").strip().lower(),
        cadence=(raw.get("cadence") or "hourly").strip().lower(),
        lookback_intervals=int(raw.get("lookback_intervals", 4)),
        gap_action=(raw.get("gap_action") or "warn").strip().lower(),
        entities=_as_list(raw.get("entities")) or ["tenant_sites", "tenant_users"],
        folder_layout=(raw.get("folder_layout") or "").strip().lower(),
        tuning=tuning,
    )


def _parse_load_job(name: str, raw: Any, _resolve) -> LoadJobConfig:
    raw = raw or {}
    dest = raw.get("destination")
    if dest is not None:
        dest = str(dest).strip() if dest else None
    return LoadJobConfig(
        name=name,
        mapping=_resolve((raw.get("mapping") or "").strip()),
        destination=dest or None,
        datasource_name=(raw.get("name") or "").strip(),
        description=(raw.get("description") or "").strip(),
    )


# ---------------------------------------------------------------------------
# Default synthesizers
# ---------------------------------------------------------------------------


def _default_extraction_jobs() -> Dict[str, ExtractionJobConfig]:
    """
    Synthesize the standard 3-job extraction set when the ``extraction:`` block
    is absent.  Equivalent to writing:

      extraction:
        event_logs:    { data: event_logs }
        snapshots:     { data: snapshots }
        reference:     { data: reference }
    """
    return {
        "event_logs": ExtractionJobConfig(name="event_logs", data=DATA_EVENT_LOGS),
        "snapshots":  ExtractionJobConfig(name="snapshots",  data=DATA_SNAPSHOTS),
        "reference":  ExtractionJobConfig(name="reference",  data=DATA_REFERENCE),
    }


def _default_transform_jobs(base_dir: str, _resolve) -> Dict[str, TransformJobConfig]:
    """
    Synthesize the standard transformer set when the ``transform:`` block is absent.

    Defaults are resolved against the installed package's bundled ``mappings/``
    directory so they work regardless of where run.yml lives.
    """
    names = {
        "events":            _DEFAULT_TRANSFORM_MAPPINGS[0],
        "entity_snapshots":  _DEFAULT_TRANSFORM_MAPPINGS[1],
        "tenant_reference":  _DEFAULT_TRANSFORM_MAPPINGS[2],
    }
    return {
        name: TransformJobConfig(name=name, mapping=_bundled_mapping(path))
        for name, path in names.items()
    }


def _default_load_jobs(base_dir: str, _resolve) -> Dict[str, LoadJobConfig]:
    """
    Synthesize a single local-only loader when the ``load:`` block is absent.

    Default is resolved against the installed package's bundled ``mappings/``
    directory so it works regardless of where run.yml lives.
    """
    return {
        "default": LoadJobConfig(
            name="default",
            mapping=_bundled_mapping(_DEFAULT_LOADER_MAPPING),
            destination=None,
        ),
    }


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------


def resolve_window(job: ExtractionJobConfig) -> tuple[str, str]:
    """
    Return (start_time, end_time) ISO 8601 UTC strings for a job.

    If start_time and end_time are both set in the job, they are returned as-is.
    Otherwise a rolling window of lookback_days ending now is computed.
    """
    if job.start_time and job.end_time:
        return job.start_time, job.end_time
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    end = now.isoformat().replace("+00:00", "Z")
    start = (now - dt.timedelta(days=job.lookback_days)).isoformat().replace("+00:00", "Z")
    return start, end


def sites_as_arg(job: ExtractionJobConfig) -> str:
    """
    Convert job.sites to the string expected by --site-uris.
    "all" → "All"; a list → comma-joined.
    """
    if job.sites == "all" or job.sites is None:
        return "All"
    if isinstance(job.sites, list):
        return ",".join(job.sites)
    return str(job.sites)
