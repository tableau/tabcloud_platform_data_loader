"""
Load and validate loader mapping YAML. Resolves {storage_path}, validates tables.
Loader mapping links loader targets to transformer outputs or to a source directory:
each table specifies its source with either transformer_mappings (pipeline output)
or source_file_path (directory of CSV/Parquet to union); exactly one is required.
Upstream transformer mappings are derived only from tables that use transformer_mappings.
"""

import os
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore


# Default Hyper schema and table name used by Tableau/Pantab
DEFAULT_HYPER_SCHEMA = "Extract"
DEFAULT_HYPER_TABLE = "Extract"


# ---------------------------------------------------------------------------
# write_mode vocabulary
# ---------------------------------------------------------------------------
# Each write_mode maps to (strategy, drift_policy).
#
# strategy:     "replace"     — rewrite the whole table every run.
#               "incremental" — append rows where increment_field > last max.
#               "snapshot"    — replace only when a newer snapshot timestamp arrives.
#
# drift_policy: "allow"   — schema changes are accepted silently.
#               "fail"    — schema change raises RuntimeError before any write.
#               "rebuild" — schema change triggers a full table replace + warning.
#
# Naming convention: plain modes auto-adapt; strict_* modes fail on drift.

_WRITE_MODES: Dict[str, Tuple[str, str]] = {
    "replace":        ("replace",     "allow"),
    "strict_replace": ("replace",     "fail"),
    "append":         ("incremental", "rebuild"),
    "strict_append":  ("incremental", "fail"),
    "snapshot":       ("snapshot",    "allow"),
    "strict_snapshot": ("snapshot",   "fail"),
}

# Modes whose strategy is incremental (require increment_field)
_INCREMENTAL_MODES = frozenset(k for k, (s, _) in _WRITE_MODES.items() if s == "incremental")

# Modes whose strategy is snapshot (require snapshot_source_dir)
_SNAPSHOT_MODES = frozenset(k for k, (s, _) in _WRITE_MODES.items() if s == "snapshot")


def load_loader_mapping(path: str) -> dict:
    """
    Load loader mapping YAML from file.

    :param path: Path to YAML file (absolute or cwd-relative).
    :return: Parsed config dict.
    """
    if yaml is None:
        raise ImportError("PyYAML is required. Install with: pip install pyyaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_hyper_path(hyper_path: str, storage_path: str) -> str:
    """
    Resolve target.hyper_path, substituting {storage_path} with the given base.

    :param hyper_path: Path from config (may contain {storage_path}).
    :param storage_path: Resolved storage base path (e.g. ./platform_data).
    :return: Absolute path for the Hyper file.
    """
    if not hyper_path:
        return ""
    resolved = hyper_path.replace("{storage_path}", storage_path).replace("{storage_path}/", os.path.join(storage_path, ""))
    return os.path.abspath(resolved)


def resolve_source_path(
    source_file_path: str,
    storage_path: str,
    project_root: Optional[str] = None,
) -> str:
    """
    Resolve table.source_file_path: substitute {storage_path}, then resolve as absolute.
    Relative paths are resolved against project_root (default cwd).

    :param source_file_path: Path from config (may contain {storage_path}).
    :param storage_path: Resolved storage base path.
    :param project_root: Base for relative paths (default cwd).
    :return: Absolute path to the source directory.
    """
    if not (source_file_path or "").strip():
        return ""
    s = (source_file_path or "").strip()
    s = s.replace("{storage_path}", storage_path).replace("{storage_path}/", os.path.join(storage_path, ""))
    if not os.path.isabs(s):
        s = os.path.join(project_root or os.getcwd(), s)
    return os.path.abspath(s)


def get_transformer_mapping_paths(loader_config: dict) -> List[str]:
    """
    Derive all upstream transformer mapping paths from tables only.
    Only tables that use transformer_mappings (not source_file_path) are included.

    :param loader_config: Loaded loader mapping dict.
    :return: Deduplicated list of transformer mapping paths.
    """
    paths: List[str] = []
    for table in (loader_config.get("tables") or []):
        if (table.get("source_file_path") or "").strip():
            continue
        for p in (table.get("transformer_mappings") or []):
            if p and p not in paths:
                paths.append(p)
    return paths


def resolve_write_mode(table_dict: dict) -> Tuple[str, str, str]:
    """
    Resolve write_mode for a table config dict.

    Returns (write_mode, strategy, drift_policy).
    Defaults to "replace" when write_mode is absent or unrecognised.
    """
    raw = (table_dict.get("write_mode") or "replace").strip().lower()
    if raw not in _WRITE_MODES:
        raw = "replace"
    strategy, drift_policy = _WRITE_MODES[raw]
    return raw, strategy, drift_policy


def get_tables_with_write_modes(
    loader_config: dict,
) -> List[Tuple[str, str, str, List[str], Optional[str], Optional[str], str]]:
    """
    Return list of per-table configuration tuples:
      (table_name, strategy, drift_policy, transformer_mappings, increment_field,
       source_file_path, schema)

    strategy:     "replace" | "incremental" | "snapshot"
    drift_policy: "allow" | "fail" | "rebuild"

    Snapshot tables (strategy=="snapshot") also require snapshot_source_dir; use
    :func:`get_snapshot_table_configs` to retrieve the full snapshot configuration.

    :param loader_config: Loaded loader mapping dict.
    :return: List of 7-tuples as described above.
    """
    result: List[Tuple[str, str, str, List[str], Optional[str], Optional[str], str]] = []
    for t in (loader_config.get("tables") or []):
        table_name = t.get("table") or DEFAULT_HYPER_TABLE
        _write_mode, strategy, drift_policy = resolve_write_mode(t)
        mappings = t.get("transformer_mappings") or []
        increment_field = (t.get("increment_field") or "").strip() or None
        source_file_path = (t.get("source_file_path") or "").strip() or None
        schema = (t.get("schema") or DEFAULT_HYPER_SCHEMA).strip() or DEFAULT_HYPER_SCHEMA
        result.append((table_name, strategy, drift_policy, list(mappings), increment_field, source_file_path, schema))
    return result


# Backward-compatibility alias: callers that still unpack (name, load_type, mappings, inc, sfp, schema)
# can continue to use this; it returns strategy as the second element which matches the old load_type
# values ("replace", "incremental", "snapshot").
def get_tables_with_load_types(
    loader_config: dict,
) -> List[Tuple[str, str, List[str], Optional[str], Optional[str], str]]:
    """
    Deprecated alias for :func:`get_tables_with_write_modes` that returns 6-tuples
    (table_name, strategy, transformer_mappings, increment_field, source_file_path, schema)
    for callers that have not yet been updated to the 7-tuple form.
    """
    return [
        (name, strategy, mappings, inc, sfp, schema)
        for name, strategy, _dp, mappings, inc, sfp, schema
        in get_tables_with_write_modes(loader_config)
    ]


def get_snapshot_table_configs(loader_config: dict, storage_path: str) -> List[Dict[str, Any]]:
    """
    Return a list of config dicts for tables with write_mode in {snapshot, strict_snapshot}.

    Each dict has:
      - ``table``: table name
      - ``schema``: Hyper schema name
      - ``drift_policy``: "allow" (snapshot) or "fail" (strict_snapshot)
      - ``snapshot_source_dir``: resolved absolute path to the transformer snapshot output dir
      - ``snapshot_time_field``: column name for snapshot timestamp (optional; for non-passthrough)

    Snapshot tables use ``snapshot_source_dir`` (not ``transformer_mappings`` or
    ``source_file_path``).  The transformer writes one ``{EntityType}.parquet`` and a
    ``_manifest.json`` sidecar to this directory; the loader reads both.

    :param loader_config: Loaded loader mapping dict.
    :param storage_path: Storage base path for ``{storage_path}`` substitution.
    :return: List of snapshot table config dicts.
    """
    result = []
    for t in (loader_config.get("tables") or []):
        _write_mode, strategy, drift_policy = resolve_write_mode(t)
        if strategy != "snapshot":
            continue
        table_name = t.get("table") or DEFAULT_HYPER_TABLE
        schema = (t.get("schema") or DEFAULT_HYPER_SCHEMA).strip() or DEFAULT_HYPER_SCHEMA
        raw_dir = (t.get("snapshot_source_dir") or "").strip()
        if raw_dir:
            resolved_dir = raw_dir.replace("{storage_path}", storage_path)
            if not os.path.isabs(resolved_dir):
                resolved_dir = os.path.abspath(resolved_dir)
        else:
            resolved_dir = ""
        snapshot_time_field = (t.get("snapshot_time_field") or "snapshotTimestamp").strip()
        result.append({
            "table": table_name,
            "schema": schema,
            "drift_policy": drift_policy,
            "snapshot_source_dir": resolved_dir,
            "snapshot_time_field": snapshot_time_field,
        })
    return result


def get_target_config(loader_config: dict, storage_path: str) -> dict:
    """
    Get target section with hyper_path resolved.

    :param loader_config: Loaded loader mapping dict.
    :param storage_path: Storage base path for {storage_path} substitution.
    :return: Target config with resolved hyper_path.
    """
    target = (loader_config.get("target") or {}).copy()
    hp = target.get("hyper_path") or ""
    if hp:
        target["hyper_path"] = resolve_hyper_path(hp, storage_path)
    return target


def get_upstream_extractor_params(loader_config: dict) -> dict:
    """
    Get optional extractor params from upstream.extractor (for defaults/overrides).

    :param loader_config: Loaded loader mapping dict.
    :return: Dict of extractor params (start_time, end_time, site_ids, etc.).
    """
    return dict((loader_config.get("upstream") or {}).get("extractor") or {})


def get_publish_mode(loader_config: dict) -> str:
    """
    Return the publish mode from target.publish.mode.

    :return: 'upsert', 'hyper_update', or 'overwrite'. Defaults to 'upsert' when unset or unrecognised.
    """
    publish = (loader_config.get("target") or {}).get("publish") or {}
    mode = (publish.get("mode") or "upsert").strip().lower()
    return mode if mode in ("upsert", "hyper_update", "overwrite") else "upsert"


def output_filename_from_mapping_ref(mapping_ref: str, project_root: Optional[str] = None) -> str:
    """
    Resolve a transformer mapping reference (file path or output filename) to output_filename.
    If mapping_ref looks like a file path (.yaml/.yml), load it and return output_filename; else return as-is.

    :param mapping_ref: Path to mapping YAML or literal output filename.
    :param project_root: Optional base for resolving relative paths.
    :return: Output filename stem (e.g. login_analytics).
    """
    s = (mapping_ref or "").strip()
    if not s:
        return "pipeline"
    if not (s.endswith(".yaml") or s.endswith(".yml")):
        return s
    path = s
    if project_root and not os.path.isabs(path):
        path = os.path.join(project_root, path)
    if not os.path.isfile(path):
        return os.path.splitext(os.path.basename(s))[0]
    try:
        from transformer.mapping import load_mapping_config
        config = load_mapping_config(path)
        return config.get("output_filename") or "pipeline"
    except Exception:
        return os.path.splitext(os.path.basename(s))[0]


def validate_loader_mapping(loader_config: dict, project_root: Optional[str] = None) -> List[str]:
    """
    Validate loader mapping. Returns list of error messages (empty if valid).

    :param loader_config: Loaded loader mapping dict.
    :param project_root: Optional base path for resolving relative transformer_mappings.
    :return: List of error strings.
    """
    errors: List[str] = []
    if not loader_config:
        return ["Loader mapping is empty."]
    target = loader_config.get("target") or {}
    if (target.get("type") or "").strip().lower() not in ("hyper", ""):
        errors.append("target.type must be 'hyper' (only supported type).")
    if (target.get("type") or "").strip().lower() == "hyper":
        if not (target.get("hyper_path") or "").strip():
            errors.append("target.hyper_path is required for hyper target.")
    tables = loader_config.get("tables") or []
    if not tables:
        errors.append("At least one table is required.")
    for i, t in enumerate(tables):
        raw_mode = (t.get("write_mode") or "replace").strip().lower()
        if raw_mode not in _WRITE_MODES:
            errors.append(
                f"Table {i}: invalid write_mode '{raw_mode}'. "
                f"Valid values: {sorted(_WRITE_MODES)}."
            )
            continue
        strategy, _drift = _WRITE_MODES[raw_mode]
        has_tm = bool(t.get("transformer_mappings"))
        has_sfp = bool((t.get("source_file_path") or "").strip())
        has_ssd = bool((t.get("snapshot_source_dir") or "").strip())
        if strategy == "snapshot":
            if not has_ssd:
                errors.append(
                    f"Table {i}: snapshot_source_dir is required when write_mode is '{raw_mode}'."
                )
            if has_tm or has_sfp:
                errors.append(
                    f"Table {i}: snapshot tables use snapshot_source_dir; "
                    "do not specify transformer_mappings or source_file_path."
                )
        else:
            if has_tm and has_sfp:
                errors.append(f"Table {i}: specify either transformer_mappings or source_file_path, not both.")
            elif not has_tm and not has_sfp:
                errors.append(f"Table {i}: either transformer_mappings or source_file_path is required.")
        if strategy == "incremental" and not (t.get("increment_field") or "").strip():
            errors.append(f"Table {i}: increment_field is required when write_mode is '{raw_mode}'.")
    return errors
