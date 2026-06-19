"""
Upstream orchestration: when --ensure-upstream, run extractor then transformer so load has data.
Loader maps to transformer inputs; transformer maps to extractor. Extractor runs with full scope
for data feeding replace tables and incrementally (missing data) for data feeding incremental tables.
"""

import os
import subprocess
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from loader.loader_mapping import (
    get_transformer_mapping_paths,
    get_tables_with_write_modes,
    output_filename_from_mapping_ref,
)
from loader.logging_utils import get_logger

logger = get_logger("orchestrate")


def get_event_types_from_transformer_mapping(mapping_path: str, project_root: Optional[str] = None) -> List[str]:
    """
    Load transformer mapping and return list of eventType values from inputs.

    :param mapping_path: Path to mapping YAML (or pipeline name; then we need to find file).
    :param project_root: Base for resolving path.
    :return: List of event type strings.
    """
    path = mapping_path
    if project_root and not os.path.isabs(path):
        path = os.path.join(project_root, path)
    if not path.endswith(".yaml") and not path.endswith(".yml"):
        return []
    try:
        from transformer.mapping import load_mapping_config
        config = load_mapping_config(path)
    except Exception:
        return []
    inputs = config.get("inputs") or []
    return [str(inp.get("eventType") or "").strip() for inp in inputs if inp.get("eventType")]


def get_required_pipelines_and_event_types(
    loader_config: dict,
    project_root: Optional[str] = None,
) -> Tuple[List[str], Set[str], Dict[str, str]]:
    """
    From loader config, resolve required transformer mapping paths and event types.
    Also returns pipeline_name -> load_type for each mapping (replace vs incremental).
    If a pipeline feeds both replace and incremental tables, we treat as replace (full scope).

    :param loader_config: Loaded loader mapping.
    :param project_root: Base for resolving paths.
    :return: (list of mapping paths, set of event types, dict mapping_path -> load_type).
    """
    mapping_to_load_type: Dict[str, str] = {}
    for table_name, strategy, _drift, mappings, _inc, _source_path, _schema in get_tables_with_write_modes(loader_config):
        if mappings:
            for m in mappings:
                mapping_to_load_type[m] = strategy
    paths = get_transformer_mapping_paths(loader_config)
    if not paths:
        return [], set(), {}
    event_types: Set[str] = set()
    for p in paths:
        for et in get_event_types_from_transformer_mapping(p, project_root):
            if et:
                event_types.add(et)
    return paths, event_types, mapping_to_load_type


def run_extractor(
    storage_path: str,
    pat_name: str,
    pat_secret: str,
    api_url: str,
    start_time: str,
    end_time: str,
    event_types: Optional[List[str]] = None,
    site_ids: Optional[str] = None,
    retrieve_tenant_logs: bool = True,
    extract_path_layout: Optional[str] = None,
    project_root: Optional[str] = None,
    storage_config_path: Optional[str] = None,
    log_dir: Optional[str] = None,
    log_retention_days: Optional[int] = None,
) -> int:
    """
    Run extractor as subprocess. Returns exit code (0 = success).

    :param project_root: Working directory for subprocess (default cwd).
    """
    project_root = project_root or os.getcwd()
    cmd = [
        sys.executable, "-m", "extractor",
        "--pat-name", pat_name,
        "--pat-secret", pat_secret,
        "--api-url", api_url,
        "--start-time", start_time,
        "--end-time", end_time,
        "--storage-path", storage_path,
    ]
    if site_ids:
        cmd.extend(["--site-ids", site_ids])
    if not retrieve_tenant_logs:
        cmd.append("--no-retrieve-tenant-logs")
    if event_types:
        cmd.extend(["--event-type", ",".join(event_types)])
    if extract_path_layout:
        cmd.extend(["--extract-path-layout", extract_path_layout])
    if storage_config_path:
        cmd.extend(["--storage-config", storage_config_path])
    if log_dir is not None:
        cmd.extend(["--log-dir", log_dir])
    if log_retention_days is not None:
        cmd.extend(["--log-retention-days", str(log_retention_days)])
    proc = subprocess.run(cmd, cwd=project_root)
    return proc.returncode


def run_transformer(
    storage_path: str,
    mapping_path: str,
    project_root: Optional[str] = None,
    storage_config_path: Optional[str] = None,
    log_dir: Optional[str] = None,
    log_retention_days: Optional[int] = None,
) -> int:
    """
    Run transformer as subprocess for one mapping. Returns exit code.
    """
    project_root = project_root or os.getcwd()
    path = mapping_path
    if not os.path.isabs(path):
        path = os.path.join(project_root, path)
    cmd = [
        sys.executable, "-m", "transformer",
        "--storage-path", storage_path,
        "--mapping", path,
    ]
    if storage_config_path:
        cmd.extend(["--storage-config", storage_config_path])
    if log_dir is not None:
        cmd.extend(["--log-dir", log_dir])
    if log_retention_days is not None:
        cmd.extend(["--log-retention-days", str(log_retention_days)])
    proc = subprocess.run(cmd, cwd=project_root)
    return proc.returncode


def run_upstream(
    loader_config: dict,
    storage_path: str,
    extractor_params: Dict[str, Any],
    project_root: Optional[str] = None,
    storage_config_path: Optional[str] = None,
    log_dir: Optional[str] = None,
    log_retention_days: Optional[int] = None,
) -> bool:
    """
    When --ensure-upstream: run extractor (always) then transformer for each required mapping.
    extractor_params must include: pat_name, pat_secret, api_url, start_time, end_time;
    optional: site_ids, retrieve_tenant_logs, event_types (overrides derived), extract_path_layout.

    :param loader_config: Loaded loader mapping.
    :param storage_path: Storage base path.
    :param extractor_params: Params for extractor (from CLI or upstream.extractor).
    :param project_root: Base for resolving mapping paths.
    :return: True if all steps succeeded.
    """
    project_root = project_root or os.getcwd()
    paths, event_types_set, _ = get_required_pipelines_and_event_types(loader_config, project_root)
    logger.info("Running extractor then %d transformer(s): %s", len(paths), paths)
    event_types = extractor_params.get("event_types")
    if event_types is None:
        event_types = list(event_types_set) if event_types_set else None
    required = [
        extractor_params.get("pat_name"),
        extractor_params.get("pat_secret"),
        extractor_params.get("api_url"),
        extractor_params.get("start_time"),
        extractor_params.get("end_time"),
    ]
    if not all(required):
        return False
    code = run_extractor(
        storage_path=storage_path,
        pat_name=extractor_params["pat_name"],
        pat_secret=extractor_params["pat_secret"],
        api_url=extractor_params["api_url"],
        start_time=extractor_params["start_time"],
        end_time=extractor_params["end_time"],
        event_types=event_types,
        site_ids=extractor_params.get("site_ids"),
        retrieve_tenant_logs=extractor_params.get("retrieve_tenant_logs", True),
        extract_path_layout=extractor_params.get("extract_path_layout"),
        project_root=project_root,
        storage_config_path=storage_config_path,
        log_dir=log_dir,
        log_retention_days=log_retention_days,
    )
    if code != 0:
        logger.error("Extractor exited with code %s", code)
        return False
    for mapping_path in paths:
        logger.info("Running transformer: %s", mapping_path)
        code = run_transformer(
            storage_path,
            mapping_path,
            project_root,
            storage_config_path=storage_config_path,
            log_dir=log_dir,
            log_retention_days=log_retention_days,
        )
        if code != 0:
            logger.error("Transformer %s exited with code %s", mapping_path, code)
            return False
    return True
