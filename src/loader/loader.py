"""
Load Parquet files into the target (incremental or bulk).
Hyper target: Pantab write to local .hyper, optional Tableau publish.
Incremental is insert-only, keyed by increment_field (min/max range); no update/delete.
"""

import glob
import os
from typing import Any, Dict, List, Optional, Tuple

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import csv
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore
    csv = None  # type: ignore

from loader.loader_mapping import (
    get_publish_mode,
    get_target_config,
    get_tables_with_write_modes,
    get_snapshot_table_configs,
    output_filename_from_mapping_ref,
    resolve_source_path,
)
from loader.targets.hyper import (
    DEFAULT_SCHEMA,
    DEFAULT_TABLE,
    hyper_file_exists,
    read_hyper_table_max,
    write_parquet_to_hyper,
    write_table_to_hyper,
    get_hyper_state,
    get_hyper_table_columns,
    diff_hyper_schema,
    drop_hyper_table,
)
from loader.diff import get_new_rows_and_max, get_new_rows_and_max_table
from loader.load_state import get_last_snapshot_time, set_last_snapshot_time
from loader.logging_utils import get_logger

logger = get_logger("loader")


def _read_snapshot_manifest(snapshot_source_dir: str) -> dict:
    """
    Read the sidecar manifest written by the transformer's passthrough pipeline.

    The manifest lives at ``{snapshot_source_dir}/_manifest.json`` and has the shape::

        {
          "Users":  {"snapshot_time": "2026-06-08T14:00:00Z", "filename": "Users.parquet", ...},
          "Groups": {"snapshot_time": "2026-06-08T14:00:00Z", "filename": "Groups.parquet", ...},
          ...
        }

    Returns an empty dict if the manifest is missing or unreadable.
    """
    manifest_path = os.path.join(snapshot_source_dir, "_manifest.json")
    if not os.path.isfile(manifest_path):
        logger.warning("Snapshot manifest not found: %s", manifest_path)
        return {}
    try:
        import json as _json
        with open(manifest_path, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception as e:
        logger.warning("Failed to read snapshot manifest %s: %s", manifest_path, e)
        return {}


def load_snapshot_tables(
    loader_config: dict,
    storage_path: str,
    hyper_path: str,
    first_table: bool,
    payload_tables: List[tuple],
    any_first_run_ref: List[bool],
    publish_mode: str,
) -> Tuple[int, bool]:
    """
    Load ``load_type: snapshot`` tables: full replace only when the snapshot is newer than
    the stored watermark.

    This implements "incremental at the file level, full replace at the table level":
    - Each entity type's snapshot time (from the transformer sidecar manifest) is compared
      against the per-table watermark stored in the load state JSON sidecar.
    - Newer snapshot → full table replace → update watermark.
    - Same or older snapshot → skip (no Hyper write, no publish action).

    Multiple snapshot tables can coexist with incremental event tables in one Hyper data
    source because the Update Data in Hyper API performs per-table insert/replace actions.

    :param loader_config: Loaded loader mapping dict.
    :param storage_path: Storage base path for {storage_path} substitution.
    :param hyper_path: Path to the target .hyper file (watermark sidecar lives alongside it).
    :param first_table: True if no table has been written to hyper_path yet in this run.
    :param payload_tables: Accumulator for hyper_update/upsert batch payload.
    :param any_first_run_ref: Mutable list([bool]) shared with caller; set to True if any
        snapshot table is a first-run (no existing watermark).
    :param publish_mode: "upsert", "hyper_update", or "overwrite".
    :returns: (total_rows_written, updated_first_table).
    """
    snapshot_configs = get_snapshot_table_configs(loader_config, storage_path)
    total_rows = 0

    for snap_cfg in snapshot_configs:
        table_name = snap_cfg["table"]
        schema = snap_cfg["schema"]
        snap_dir = snap_cfg["snapshot_source_dir"]

        if not snap_dir:
            logger.warning("Table %s: snapshot_source_dir is empty; skipping.", table_name)
            continue
        if not os.path.isdir(snap_dir):
            logger.warning("Table %s: snapshot_source_dir not found: %s; skipping.", table_name, snap_dir)
            continue

        manifest = _read_snapshot_manifest(snap_dir)
        if not manifest:
            logger.info("Table %s: no snapshot manifest found; skipping.", table_name)
            continue

        entry = manifest.get(table_name)
        if not entry:
            logger.debug(
                "Table %s: not found in manifest (available: %s); skipping.",
                table_name, sorted(manifest.keys()),
            )
            continue

        manifest_snapshot_time = (entry.get("snapshot_time") or "").strip()
        parquet_filename = (entry.get("filename") or "").strip()
        if not parquet_filename:
            logger.warning("Table %s: manifest entry missing 'filename'; skipping.", table_name)
            continue

        parquet_path = os.path.join(snap_dir, parquet_filename)
        if not os.path.isfile(parquet_path):
            logger.warning("Table %s: Parquet file not found: %s; skipping.", table_name, parquet_path)
            continue
        if os.path.getsize(parquet_path) == 0:
            logger.warning(
                "Table %s: Parquet file is empty (0 bytes): %s; skipping. "
                "This usually means the transformer produced an empty output for this entity type.",
                table_name, parquet_path,
            )
            continue

        last_watermark = get_last_snapshot_time(hyper_path, table_name)

        if last_watermark and manifest_snapshot_time and manifest_snapshot_time <= last_watermark:
            logger.info(
                "Table %s: snapshot not newer than watermark (%s ≤ %s); skipping.",
                table_name, manifest_snapshot_time, last_watermark,
            )
            continue

        is_first_run = last_watermark is None
        if is_first_run:
            any_first_run_ref[0] = True

        drift_policy = snap_cfg.get("drift_policy", "allow")

        # Probe the existing table schema once (None when table/file absent).
        # Reused for both the strict drift check and the drop-before-replace below.
        existing_cols = (
            get_hyper_table_columns(hyper_path, schema, table_name)
            if not is_first_run and hyper_file_exists(hyper_path)
            else None
        )

        # Drift check for strict_snapshot: halt BEFORE any write so the watermark
        # is not advanced; the next run will re-detect the same new snapshot and
        # fail again until the schema mismatch is resolved.
        if drift_policy == "fail" and existing_cols is not None and pa is not None:
            incoming_arrow = pq.read_table(parquet_path)
            _check_for_schema_drift(existing_cols, incoming_arrow, table_name, drift_policy)

        # True full replace: drop the existing table before writing so pantab never
        # sees a column-count mismatch (append requires an exact schema match).
        # This applies to both "snapshot" (allow) and "strict_snapshot" (fail) — the
        # drift policy above is a pre-write gate; once it passes the table must still
        # be fully replaced, not appended.
        mode = "w" if first_table else "a"
        if mode == "a" and existing_cols is not None:
            drop_hyper_table(hyper_path, schema, table_name)

        try:
            n = write_parquet_to_hyper(parquet_path, hyper_path, table=table_name, table_mode=mode, schema=schema)
        except Exception as e:
            logger.warning(
                "Table %s: failed to write snapshot Parquet %s — skipping table. "
                "The watermark will NOT be advanced. Error: %s",
                table_name, parquet_path, e,
            )
            continue
        total_rows += n
        logger.info(
            "Table %s: snapshot replace (%s), wrote %d rows (snapshot_time=%s, prev_watermark=%s)",
            table_name, "first run" if is_first_run else "new snapshot",
            n, manifest_snapshot_time or "unknown", last_watermark or "none",
        )
        first_table = False

        if publish_mode in ("hyper_update", "upsert") and pa is not None:
            try:
                arrow_table = pq.read_table(parquet_path)
                payload_tables.append((arrow_table, "replace", table_name, schema))
            except Exception as e:
                logger.warning("Could not read %s for payload: %s", parquet_path, e)

        if manifest_snapshot_time:
            set_last_snapshot_time(hyper_path, table_name, manifest_snapshot_time)

    return total_rows, first_table


def load_parquet(paths: List[str], target_config: dict, mode: str = "bulk") -> int:
    """
    Load Parquet file(s) into the target.

    :param paths: List of paths to Parquet files
    :param target_config: Target-specific config (type, hyper_path, etc.)
    :param mode: 'bulk' or 'incremental'
    :return: Number of rows loaded (or 0 if not applicable)
    """
    target_type = (target_config.get("type") or "").strip().lower()
    if target_type == "hyper":
        total = 0
        hyper_path = target_config.get("hyper_path") or ""
        if not hyper_path or not paths:
            return 0
        for p in paths:
            n = _write_single_parquet_to_hyper(p, hyper_path, table_mode="a" if total else "w")
            total += n
        return total
    return 0



def _write_single_parquet_to_hyper(
    parquet_path: str,
    hyper_path: str,
    table: str = DEFAULT_TABLE,
    table_mode: str = "w",
    schema: str = DEFAULT_SCHEMA,
) -> int:
    return write_parquet_to_hyper(parquet_path, hyper_path, table=table, table_mode=table_mode, schema=schema)


def _read_tables_from_directory(dir_path: str) -> "pa.Table":
    """
    Read all .parquet and .csv files from a directory, union them, and return a single PyArrow Table.
    Empty directory or no matching files returns an empty table (0 rows, 0 columns).

    :param dir_path: Absolute path to directory.
    :return: Unioned PyArrow Table (empty if no files or on error).
    """
    if pa is None:
        raise ImportError("pyarrow is required for source_file_path. Install with: pip install pyarrow")
    if not os.path.isdir(dir_path):
        return pa.table({})
    tables: List["pa.Table"] = []
    for ext in ("*.parquet", "*.csv"):
        for path in sorted(glob.glob(os.path.join(dir_path, ext))):
            if not os.path.isfile(path):
                continue
            try:
                if path.lower().endswith(".parquet"):
                    tables.append(pq.read_table(path))
                else:
                    if csv is None:
                        raise ImportError("pyarrow.csv is required for CSV source_file_path. Install with: pip install pyarrow")
                    tables.append(csv.read_csv(path))
            except Exception as e:
                logger.warning("Skipping %s: %s", path, e)
    if not tables:
        return pa.table({})
    if len(tables) == 1:
        return tables[0]
    return pa.concat_tables(tables, promote_options="permissive")


def _check_for_schema_drift(
    existing_cols: Optional[list],
    incoming_table: "pa.Table",
    table_name: str,
    drift_policy: str,
) -> bool:
    """
    Apply the configured drift policy when an existing Hyper table schema differs from
    the incoming PyArrow table.  Called just before a write; ``existing_cols`` comes from
    :func:`~loader.targets.hyper.get_hyper_table_columns`.

    Returns ``True`` when the caller should perform a full table rebuild instead of a
    normal append (``rebuild`` policy with drift detected).  Returns ``False`` for no-op
    cases: policy is ``allow``, the table did not previously exist, or no drift was found.

    Raises ``RuntimeError`` when policy is ``fail`` and drift is detected, listing every
    added, removed, and type-changed column so the operator knows exactly what changed.
    """
    if drift_policy == "allow" or existing_cols is None:
        return False
    diff = diff_hyper_schema(existing_cols, incoming_table)
    if diff is None:
        return False  # schemas match; nothing to do
    added = diff.get("added", [])
    removed = diff.get("removed", [])
    type_changed = diff.get("type_changed", [])
    parts = []
    if added:
        parts.append(f"added columns: {added}")
    if removed:
        parts.append(f"removed columns: {removed}")
    if type_changed:
        parts.append(
            "type changes: " + ", ".join(f"{c}: {e!s} -> {i!s}" for c, e, i in type_changed)
        )
    drift_summary = f"Table {table_name!r}: schema drift detected — {'; '.join(parts)}"
    if drift_policy == "fail":
        raise RuntimeError(
            drift_summary
            + ". Use write_mode='replace' or 'append' to auto-adapt, or update the "
            "source schema to match the existing Hyper table."
        )
    # drift_policy == "rebuild": drop + rewrite instead of append
    logger.warning(
        "%s — dropping and rebuilding the table (write_mode=append).",
        drift_summary,
    )
    return True


def load_hyper(
    loader_config: dict,
    storage_path: str,
    transform_path: str,
    project_root: Optional[str] = None,
    publish_options: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Run Hyper load from loader mapping: resolve tables, write Parquet -> Hyper (replace or incremental),
    optionally publish to Tableau.

    publish.mode controls how Tableau is updated (independent of per-table load_type):
      "upsert"       — PATCH if datasource exists; falls back to full publish if not found or
                       PATCH fails. Best for scheduled/unattended runs. (default)
      "hyper_update" — PATCH /datasources/{id}/data modifying only the underlying data.
                       Fails if datasource not found on server (no fallback).
      "overwrite"    — full POST publish replacing the entire datasource every run.

    For upsert/hyper_update modes, all table deltas are batched into a single payload Hyper
    file and sent in one PATCH request. First-run tables (no existing local Hyper) fall back
    to a full publish for that run only.

    :param loader_config: Loaded loader mapping dict.
    :param storage_path: Storage base path.
    :param transform_path: Path to transform/ (Parquet files).
    :param project_root: Optional base for resolving transformer_mappings to pipeline names.
    :param publish_options: Optional dict with server_url, token_name, token_secret, site, project, datasource_name, tds_path.
    :return: Total rows written to Hyper.
    """
    target = get_target_config(loader_config, storage_path)
    if (target.get("type") or "").strip().lower() != "hyper":
        logger.debug("Target type is not hyper; skipping.")
        return 0
    hyper_path = target.get("hyper_path") or ""
    if not hyper_path:
        return 0
    logger.info("Starting Hyper load to %s", hyper_path)
    # publish_mode: prefer explicit runner override > loader mapping value > default "upsert"
    if publish_options:
        publish_mode = (
            publish_options.get("publish_mode_override")
            or get_publish_mode(loader_config)
            or "upsert"
        )
    else:
        publish_mode = "upsert"
    destination_name = (publish_options or {}).get("destination_name") or None
    tables_config = get_tables_with_write_modes(loader_config)
    project_root = project_root or os.getcwd()
    total_rows = 0
    first_table = True
    # For hyper_update mode: accumulate (arrow_table, action, table_name, schema) for batch PATCH.
    payload_tables: List[tuple] = []
    any_first_run = False  # any table had no existing local data; needs full publish fallback

    # Pre-probe existing Hyper schemas for strict_replace tables BEFORE any writes.
    # The first replace table uses table_mode="w" which recreates the file, so we must
    # capture the old schemas now while they are still readable.
    pre_probed_schemas: Dict[str, Optional[list]] = {}
    if hyper_file_exists(hyper_path):
        for _tn, _strat, _dp, _maps, _inc, _sfp, _tschema in tables_config:
            if _strat == "replace" and _dp == "fail":
                pre_probed_schemas[_tn] = get_hyper_table_columns(hyper_path, _tschema, _tn)

    for table_name, strategy, drift_policy, transformer_mappings, increment_field, source_file_path, schema in tables_config:
        if source_file_path:
            # Source: directory of CSV/Parquet files to union
            dir_path = resolve_source_path(source_file_path, storage_path, project_root)
            if not os.path.isdir(dir_path):
                logger.warning("Table %s: source_file_path directory not found: %s; skipping.", table_name, dir_path)
                continue
            if strategy == "incremental" and not increment_field:
                logger.warning("Table %s has incremental write_mode but no increment_field; skipping.", table_name)
                continue
            if pa is None:
                raise ImportError("pyarrow is required for source_file_path. Install with: pip install pyarrow")
            table = _read_tables_from_directory(dir_path)
            if table.num_rows == 0:
                logger.debug("Table %s: no CSV/Parquet files in %s; skipping.", table_name, dir_path)
                continue
            if strategy == "replace":
                existing_cols = pre_probed_schemas.get(table_name)
                _check_for_schema_drift(existing_cols, table, table_name, drift_policy)
                mode = "w" if first_table else "a"
                n = write_table_to_hyper(table, hyper_path, table_name=table_name, table_mode=mode, schema=schema)
                total_rows += n
                logger.info("Table %s: replace (source_file_path), wrote %d rows from %s", table_name, n, dir_path)
                first_table = False
                if publish_mode in ("hyper_update", "upsert"):
                    payload_tables.append((table, "replace", table_name, schema))
            else:
                last_max = read_hyper_table_max(hyper_path, table_name, increment_field, schema=schema)
                new_rows, _ = get_new_rows_and_max_table(table, increment_field, last_max)
                if last_max is None:
                    n = write_table_to_hyper(table, hyper_path, table_name=table_name, table_mode="w", schema=schema)
                    total_rows += n
                    logger.info("Table %s: incremental first run (source_file_path), %d rows (increment_field=%s)", table_name, n, increment_field)
                    first_table = False
                    any_first_run = True
                elif new_rows.num_rows > 0:
                    existing_cols = get_hyper_table_columns(hyper_path, schema, table_name)
                    needs_rebuild = _check_for_schema_drift(existing_cols, new_rows, table_name, drift_policy)
                    if needs_rebuild:
                        drop_hyper_table(hyper_path, schema, table_name)
                        n = write_table_to_hyper(new_rows, hyper_path, table_name=table_name, table_mode="a", schema=schema)
                        any_first_run = True
                    else:
                        n = write_table_to_hyper(new_rows, hyper_path, table_name=table_name, table_mode="a", schema=schema)
                    total_rows += n
                    logger.info("Table %s: incremental (source_file_path), %d new rows (increment_field=%s)", table_name, n, increment_field)
                    first_table = False
                    if publish_mode in ("hyper_update", "upsert"):
                        payload_tables.append((new_rows, "insert", table_name, schema))
                else:
                    logger.debug("Table %s: incremental (source_file_path), no new rows", table_name)
            continue

        if not transformer_mappings:
            continue
        if strategy == "incremental" and not increment_field:
            logger.warning("Table %s has incremental write_mode but no increment_field; skipping.", table_name)
            continue
        pipeline_name = output_filename_from_mapping_ref(transformer_mappings[0], project_root)
        parquet_path = os.path.join(transform_path, f"{pipeline_name}.parquet")
        if not os.path.isfile(parquet_path):
            logger.debug("Parquet not found for table %s: %s; skipping.", table_name, parquet_path)
            continue
        if strategy == "replace":
            # For strict_replace, check against the pre-probed schema before any write.
            existing_cols = pre_probed_schemas.get(table_name)
            if existing_cols is not None:
                if pa is None:
                    raise ImportError("pyarrow is required for schema drift check. Install with: pip install pyarrow")
                _check_for_schema_drift(existing_cols, pq.read_table(parquet_path), table_name, drift_policy)
            mode = "w" if first_table else "a"
            n = write_parquet_to_hyper(parquet_path, hyper_path, table=table_name, table_mode=mode, schema=schema)
            total_rows += n
            logger.info("Table %s: replace, wrote %d rows from %s", table_name, n, parquet_path)
            first_table = False
            if publish_mode in ("hyper_update", "upsert"):
                if pa is None:
                    raise ImportError("pyarrow is required for hyper_update/upsert publish mode")
                payload_tables.append((pq.read_table(parquet_path), "replace", table_name, schema))
        else:
            if pa is None:
                raise ImportError("pyarrow is required for incremental load")
            last_max = read_hyper_table_max(hyper_path, table_name, increment_field, schema=schema)
            new_rows, _ = get_new_rows_and_max(parquet_path, increment_field, last_max)
            if last_max is None:
                n = write_parquet_to_hyper(parquet_path, hyper_path, table=table_name, table_mode="w", schema=schema)
                total_rows += n
                logger.info("Table %s: incremental first run, replaced with %d rows (increment_field=%s)", table_name, n, increment_field)
                first_table = False
                any_first_run = True
            elif new_rows.num_rows > 0:
                existing_cols = get_hyper_table_columns(hyper_path, schema, table_name)
                needs_rebuild = _check_for_schema_drift(existing_cols, new_rows, table_name, drift_policy)
                if needs_rebuild:
                    drop_hyper_table(hyper_path, schema, table_name)
                    n = write_table_to_hyper(new_rows, hyper_path, table_name=table_name, table_mode="a", schema=schema)
                    any_first_run = True
                else:
                    n = write_table_to_hyper(new_rows, hyper_path, table_name=table_name, table_mode="a", schema=schema)
                total_rows += n
                logger.info("Table %s: incremental, %d new rows (increment_field=%s, last_max=%s)", table_name, n, increment_field, last_max)
                first_table = False
                if publish_mode == "hyper_update":
                    payload_tables.append((new_rows, "insert", table_name, schema))
            else:
                logger.debug("Table %s: incremental, no new rows (increment_field=%s, last_max=%s)", table_name, increment_field, last_max)

    # ---- Snapshot tables (load_type: snapshot) ----
    # These tables are processed separately because they read from the transformer's
    # snapshot output directory (with sidecar manifest) rather than a named pipeline output.
    # Mixed event+snapshot loads coexist via per-table replace actions in the Hyper file.
    any_first_run_ref = [any_first_run]
    snap_rows, first_table = load_snapshot_tables(
        loader_config=loader_config,
        storage_path=storage_path,
        hyper_path=hyper_path,
        first_table=first_table,
        payload_tables=payload_tables,
        any_first_run_ref=any_first_run_ref,
        publish_mode=publish_mode,
    )
    total_rows += snap_rows
    any_first_run = any_first_run_ref[0]

    publish_failed = False
    # Determine whether to publish: either new data was written this run, or
    # the destination has never been successfully published (published_through is None).
    # This ensures a prior publish failure is automatically retried next run.
    should_publish = False
    if publish_options and hyper_path and destination_name:
        from loader.load_state import get_published_through
        last_published = get_published_through(hyper_path, destination_name)
        should_publish = (total_rows > 0) or (last_published is None)
        if not should_publish:
            logger.info(
                "Destination '%s': no new data and previous publish succeeded (%s); skipping publish.",
                destination_name, last_published,
            )
    elif publish_options and hyper_path:
        # No destination_name tracking — always publish when publish_options is set.
        should_publish = True
    if publish_options and hyper_path and should_publish:
        if publish_mode == "hyper_update":
            if any_first_run:
                logger.info(
                    "hyper_update mode: first-run table(s) detected; falling back to full publish for this run. "
                    "Subsequent runs will use in-place update."
                )
                if not _publish_hyper_if_needed(loader_config, hyper_path, publish_options, project_root=project_root):
                    publish_failed = True
            if payload_tables and not publish_failed:
                patch_ok = _do_hyper_update(payload_tables, hyper_path, publish_options, fallback_publish=False)
                if not patch_ok:
                    logger.error(
                        "hyper_update publish mode: in-place update failed, and no full-publish fallback is configured. "
                        "Local Hyper file is updated, but Tableau datasource data may be stale."
                    )
                    publish_failed = True
        elif publish_mode == "upsert":
            if any_first_run:
                logger.info(
                    "upsert mode: first-run table(s) detected; performing full publish for this run. "
                    "Subsequent runs will use in-place update."
                )
                if not _publish_hyper_if_needed(loader_config, hyper_path, publish_options, project_root=project_root):
                    publish_failed = True
            elif payload_tables:
                patch_ok = _do_hyper_update(payload_tables, hyper_path, publish_options, fallback_publish=True)
                if not patch_ok:
                    logger.warning(
                        "upsert publish mode: in-place update failed; falling back to full publish."
                    )
                    if not _publish_hyper_if_needed(loader_config, hyper_path, publish_options, project_root=project_root):
                        publish_failed = True
            else:
                if not _publish_hyper_if_needed(loader_config, hyper_path, publish_options, project_root=project_root):
                    publish_failed = True
        else:
            if not _publish_hyper_if_needed(loader_config, hyper_path, publish_options, project_root=project_root):
                publish_failed = True

    # Advance published_through ONLY after a successful publish.
    if publish_options and hyper_path and destination_name and not publish_failed and should_publish:
        import datetime as _dt
        from loader.load_state import set_published_through
        ts = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        set_published_through(hyper_path, destination_name, ts)
        logger.debug("Published_through updated for destination '%s': %s", destination_name, ts)

    if publish_failed:
        logger.error("Hyper load completed locally, but Tableau publish failed.")
        return -1
    logger.info("Hyper load complete: %d total rows written → %s", total_rows, hyper_path)
    return total_rows


def _do_hyper_update(
    payload_tables: List[tuple],
    hyper_path: str,
    publish_options: Dict[str, Any],
    fallback_publish: bool = False,
) -> bool:
    """
    Batch PATCH update of a published Tableau datasource via the Update Data in Hyper Data Source API.

    Builds a single multi-table payload Hyper file from all accumulated table deltas, uploads it in
    one session, and sends one PATCH request with one action per table. When fallback_publish is True,
    a missing datasource is published as a new datasource instead of failing.

    :param payload_tables: List of (arrow_table, action, table_name, schema) accumulated during the loop.
                           action is "insert" (incremental append) or "replace" (full table swap).
    :param hyper_path: Path to the local Hyper file (used to derive the datasource name).
    :param publish_options: Dict with server_url, token_name, token_secret, site, project, datasource_name.
    :param fallback_publish: If True and datasource not found, do a full publish instead of failing.
    :return: True if update (or fallback publish) succeeded, False otherwise.
    """
    from loader.targets.tableau_publish import TableauPublishError
    from loader.targets import tableau_publish as tp
    payload_path = os.path.join(os.path.dirname(hyper_path) or ".", "payload_update.hyper")
    inputs_for_errors = {
        "server_url": publish_options.get("server_url"),
        "site": publish_options.get("site", ""),
        "project": publish_options.get("project", ""),
        "payload_path": payload_path,
    }
    try:
        # Write each table delta into the payload Hyper under its own name.
        first_payload = True
        for arrow_table, _action, table_name, schema in payload_tables:
            mode = "w" if first_payload else "a"
            write_table_to_hyper(arrow_table, payload_path, table_name=table_name, table_mode=mode, schema=schema)
            first_payload = False

        auth, site_id_from_signin = tp.sign_in(
            publish_options["server_url"],
            publish_options["token_name"],
            publish_options["token_secret"],
            publish_options.get("site", ""),
        )
        site_id = site_id_from_signin or tp.resolve_site_id(
            publish_options["server_url"], auth, publish_options.get("site", "")
        )
        project_id = tp.resolve_project_id(
            publish_options["server_url"], auth, site_id, publish_options.get("project", ""),
            create_missing=True,
        )
        ds_name = publish_options.get("datasource_name") or os.path.splitext(os.path.basename(hyper_path))[0]
        ds_list = tp.query_datasources(publish_options["server_url"], auth, site_id, name=ds_name)
        logger.debug(
            "query_datasources returned %d datasource(s): %s",
            len(ds_list),
            [(ds.get("name"), (ds.get("project") or {}).get("id")) for ds in ds_list],
        )
        datasource_id = None
        for ds in ds_list:
            if ds.get("name") == ds_name and (ds.get("project") or {}).get("id") == project_id:
                datasource_id = ds.get("id")
                break
        if not datasource_id:
            for ds in ds_list:
                if ds.get("name") == ds_name:
                    datasource_id = ds.get("id")
                    break
        if not datasource_id:
            if fallback_publish:
                description = publish_options.get("description")
                tds_path = publish_options.get("tds_path")
                if tds_path and not os.path.isabs(tds_path):
                    tds_path = os.path.abspath(tds_path)
                tp.publish_hyper_to_tableau(
                    publish_options["server_url"],
                    auth,
                    site_id,
                    project_id,
                    hyper_path,
                    ds_name,
                    overwrite=False,
                    tds_path=tds_path,
                    description=description,
                )
                logger.info(
                    "Datasource '%s' was not found on Tableau; published a new datasource instead.",
                    ds_name,
                )
                return True
            logger.error(
                "Hyper update failed: datasource '%s' not found on server (site=%s, project=%s). "
                "Local Hyper IS updated; re-run will retry the PATCH.",
                ds_name,
                publish_options.get("site", ""),
                publish_options.get("project", ""),
            )
            return False
        upload_id = tp.initiate_file_upload(publish_options["server_url"], auth, site_id)
        tp.append_to_file_upload(publish_options["server_url"], auth, site_id, upload_id, payload_path)
        actions = [
            {
                "action": action,
                "source-schema": schema,
                "source-table": table_name,
                "target-schema": schema,
                "target-table": table_name,
            }
            for _arrow_table, action, table_name, schema in payload_tables
        ]
        resp = tp.update_hyper_data(publish_options["server_url"], auth, site_id, datasource_id, upload_id, actions)
        table_summary = ", ".join(
            f"{table_name}({action})" for _t, action, table_name, _s in payload_tables
        )
        job_id = (resp.get("job") or {}).get("id") if resp else None
        if job_id:
            logger.info("Hyper update PATCH accepted (jobId=%s): %s", job_id, table_summary)
            success, notes = tp.wait_for_job(
                publish_options["server_url"], auth, site_id, job_id
            )
            if success:
                logger.info("Hyper update job succeeded (jobId=%s): %s", job_id, table_summary)
                return True
            logger.error(
                "Hyper update job failed (jobId=%s): %s. Notes: %s",
                job_id, table_summary, notes or "(no details)",
            )
            return False
        else:
            logger.debug("Hyper update PATCH response contained no job id; assuming accepted: %s", table_summary)
            return True
    except TableauPublishError as e:
        logger.error("%s", e)
        if e.cause is not None:
            logger.debug("Underlying cause: %s", e.cause)
        return False
    except Exception as e:
        _log_publish_failure("hyper_update", inputs_for_errors, e)
        return False
    finally:
        if os.path.isfile(payload_path):
            try:
                os.remove(payload_path)
            except OSError:
                pass


def _publish_failure_suggestion(step: str, _e: Exception) -> str:
    """Return a remediation suggestion for a failed publish step."""
    suggestions = {
        "resolve_site_id": "Check that 'site' matches a site content URL or LUID on the server (e.g. tableausalesdemo).",
        "resolve_project_id": "Check that 'project' matches a project name, path (e.g. Z_Admin Insights/PDA), or project LUID.",
        "publish_hyper_to_tableau": "Check that the project exists, you have publish permission, and that overwrite is set as intended if the datasource name already exists.",
        "initiate_file_upload": "Ensure the auth token and site are valid; retry after a short delay.",
        "append_to_file_upload": "Ensure the Hyper file exists and is not locked; check network stability.",
        "hyper_update": "Check sign-in, site, project, and that the datasource exists on the server with a live-to-Hyper connection. Ensure the PAT has the tableau:hyper_data:update scope (API 3.27+).",
        "publish_datasource": "Check that the project exists, you have publish permission, and that overwrite is set as intended if the datasource name already exists.",
    }
    return suggestions.get(
        step,
        "Review the exception detail and Tableau REST API docs for this operation.",
    )


def _log_publish_failure(
    step: str,
    inputs: Dict[str, Any],
    e: Exception,
) -> None:
    suggestion = _publish_failure_suggestion(step, e)
    logger.error(
        "Tableau publish failed at step: %s. What went wrong: %s. Inputs: %s. Remedy: %s",
        step,
        e,
        inputs,
        suggestion,
    )
    logger.debug("Exception detail", exc_info=True)


def _publish_hyper_if_needed(
    loader_config: dict,
    hyper_path: str,
    publish_options: Dict[str, Any],
    project_root: Optional[str] = None,
) -> bool:
    target = loader_config.get("target") or {}
    publish = target.get("publish")
    if not publish or not hyper_path:
        logger.debug("Tableau publish skipped: no publish config")
        return True
    if not os.path.isfile(hyper_path):
        logger.debug("Tableau publish skipped: hyper file not found (generate hyper before publishing)")
        return True
    from loader.targets.tableau_publish import TableauPublishError
    from loader.targets import tableau_publish as tp
    server_url = publish.get("server_url") or publish_options.get("server_url")
    token_name = publish_options.get("token_name") or publish.get("token_name")
    token_secret = publish_options.get("token_secret") or publish.get("token_secret")
    site = publish.get("site") or publish_options.get("site") or ""
    project = publish.get("project") or publish_options.get("project") or ""
    if not all([server_url, token_name, token_secret]):
        logger.warning("Tableau publish skipped: missing server_url, token_name, or token_secret")
        return False
    tds_path = publish.get("tds_path")
    if tds_path and project_root and not os.path.isabs(tds_path):
        tds_path = os.path.join(project_root, tds_path)
    logger.info("Publishing Hyper to Tableau: %s (project=%s)", server_url, project or "(default)")
    inputs_for_errors = {
        "server_url": server_url,
        "site": site or "(default)",
        "project": project or "(default)",
        "hyper_path": hyper_path,
    }
    try:
        auth, site_id_from_signin = tp.sign_in(server_url, token_name, token_secret, site)
    except TableauPublishError as e:
        logger.error("%s", e)
        if e.cause is not None:
            logger.debug("Underlying cause: %s", e.cause)
        return False
    except Exception as e:
        _log_publish_failure("sign-in", inputs_for_errors, e)
        return False
    try:
        site_id = site_id_from_signin or tp.resolve_site_id(server_url, auth, site)
    except TableauPublishError as e:
        logger.error("%s", e)
        if e.cause is not None:
            logger.debug("Underlying cause: %s", e.cause)
        return False
    except Exception as e:
        _log_publish_failure("resolve_site_id", inputs_for_errors, e)
        return False
    try:
        project_id = tp.resolve_project_id(server_url, auth, site_id, project)
    except TableauPublishError as e:
        logger.error("%s", e)
        if e.cause is not None:
            logger.debug("Underlying cause: %s", e.cause)
        return False
    except Exception as e:
        _log_publish_failure("resolve_project_id", inputs_for_errors, e)
        return False
    datasource_name = (
        publish_options.get("datasource_name")
        or publish.get("name")
        or publish.get("datasource_name")
        or os.path.splitext(os.path.basename(hyper_path))[0]
    )
    description = publish_options.get("description") or publish.get("description") or None
    inputs_for_errors["datasource_name"] = datasource_name
    try:
        tp.publish_hyper_to_tableau(
            server_url, auth, site_id, project_id, hyper_path, datasource_name,
            overwrite=publish.get("overwrite", True),
            tds_path=tds_path,
            description=description,
        )
        logger.info("Published datasource %s to Tableau successfully", datasource_name)
        return True
    except TableauPublishError as e:
        logger.error("%s", e)
        if e.cause is not None:
            logger.debug("Underlying cause: %s", e.cause)
        return False
    except Exception as e:
        _log_publish_failure("publish_hyper_to_tableau", inputs_for_errors, e)
        return False
