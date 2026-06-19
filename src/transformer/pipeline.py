"""
Orchestrate the transform pipeline: load unified mapping, for each input discover files, load
JSON, apply mappings, union all data into one table, write a single output file (Parquet or
CSV) or partitioned dataset.

Snapshot extension:
  passthrough + split_by mode: when the mapping has ``passthrough: true`` and a ``split_by``
  field (e.g. ``entityType``), each entity type's latest snapshot is selected per site, all
  part files are read with pyarrow, unioned (with a siteLuid column added), and written to
  ``{output_dir}/{PartitionValue}.parquet``.  A sidecar manifest ``_manifest.json`` is written
  with the max snapshot timestamp across sites for replace-on-new watermarking.

  For non-passthrough snapshot mappings (custom schemas), ``partition_field`` replaces the
  hardcoded "eventType" and ``snapshot_time_field`` injects a snapshotTimestamp column.

  The ``select`` key controls file selection before union (see reader.select_files_for_mapping).
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from transformer.reader import (
    discover_files_by_source,
    load_json_records,
    load_first_json_record,
    select_files_for_mapping,
    group_files_by_partition_newest_first,
    select_latest_per_site_per_partition,
    _path_to_date_tuple,
    extract_event_type_from_path,
    extract_segment_value_from_path,
    is_valid_parquet_file,
)
from transformer.mapping import (
    load_mapping_config,
    apply_input_mapping,
    apply_auto_mapping,
    get_schema_field_names,
    get_schema_types,
    is_auto_schema,
    validate_mapping_types,
    get_partition_field,
    get_select_mode,
    is_passthrough,
    get_split_by,
    get_snapshot_time_field,
)
from transformer.parquet_io import write_table_parquet
from transformer.csv_io import write_table_csv
from transformer.logging_utils import create_operation_id, log_operation

logger = logging.getLogger("transformer.pipeline")


def _write_json_atomic(path: str, data: dict, **json_kwargs) -> None:
    """Write *data* as JSON to *path* atomically (temp-write then rename)."""
    tmp = path + ".part"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, **json_kwargs)
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _effective_search_path(raw_base: str, folder_pattern: str) -> str:
    """Return the actual root directory that will be walked for this folder_pattern."""
    has_wildcard = folder_pattern and "*" in folder_pattern
    is_bare_segment = bool(folder_pattern) and "/" not in folder_pattern and not has_wildcard
    if has_wildcard or is_bare_segment or not folder_pattern or folder_pattern == "*":
        return raw_base
    return os.path.join(raw_base, folder_pattern)


def _discover_input_files(
    op_id: str,
    storage_raw,
    input_config: dict,
) -> List[str]:
    """Discover files for one input config entry and log the result."""
    source = input_config.get("source") or {}
    folder_pattern = source.get("folder_pattern") or ""
    file_pattern = source.get("file_pattern") or "*.json"
    recursive = source.get("recursive", True)
    event_type = input_config.get("eventType") or input_config.get("entityType") or ""

    raw_base = getattr(storage_raw, "base_path", "")
    search_path_full = _effective_search_path(raw_base, folder_pattern)
    files = discover_files_by_source(storage_raw, folder_pattern, file_pattern, recursive)

    log_operation(
        op_id,
        "discover_input",
        "complete",
        result={
            "status": "success",
            "eventType": event_type,
            "folder_pattern": folder_pattern,
            "search_path_full": os.path.abspath(search_path_full),
            "file_pattern": file_pattern,
            "file_count": len(files),
        },
    )
    return files


def _iso_from_date_tuple(date_tup: tuple) -> str:
    """Convert a (y, m, d, h) tuple to an ISO 8601 UTC string."""
    y, m, d, h = date_tup
    if not (y and m and d):
        return ""
    try:
        dt = datetime(y, m, d, h or 0)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return ""


def _sample_auto_schema(
    op_id: str,
    storage_raw,
    inputs: list,
    partition_field: str = "eventType",
) -> Tuple[List[str], Dict[str, str], Dict[int, List[str]]]:
    """
    Sample one record per unique partition value across all discovered files to build a schema.

    Groups files by the ``partition_field=`` segment in their path and reads the first parseable
    record from one file per unique partition value.

    Returns:
        schema_field_names: Ordered list — partition_field first, then remaining fields sorted.
        schema_types: All fields typed as "string" (safe for heterogeneous union).
        input_files_cache: Maps input list index → discovered file list (reused in process phase).
    """
    all_fields: set = set()
    sample_values_by_field: Dict[str, List[object]] = {}
    input_files_cache: Dict[int, List[str]] = {}
    sampled_partition_values: Dict[str, str] = {}   # partition_value -> sampled rel_path
    unreadable_partition_values: List[str] = []

    for i, input_config in enumerate(inputs):
        files = _discover_input_files(op_id, storage_raw, input_config)
        input_files_cache[i] = files

        by_partition: Dict[str, List[str]] = {}
        for rel_path in files:
            pv = extract_segment_value_from_path(rel_path, partition_field) or ""
            by_partition.setdefault(pv, []).append(rel_path)

        for pv, pv_files in by_partition.items():
            if pv in sampled_partition_values:
                continue
            record = None
            sampled_path = None
            for rel_path in pv_files:
                record = load_first_json_record(storage_raw, rel_path)
                if record is not None:
                    sampled_path = rel_path
                    break
            if record is not None:
                all_fields.update(record.keys())
                for k, v in record.items():
                    sample_values_by_field.setdefault(k, []).append(v)
                sampled_partition_values[pv] = sampled_path
            else:
                unreadable_partition_values.append(pv or "<unknown>")

    # The partition_field column is always first — injected from the path, not the record.
    all_fields.discard(partition_field)
    schema_field_names = [partition_field] + sorted(all_fields)
    schema_types = {partition_field: "string"}
    for f in sorted(all_fields):
        schema_types[f] = _infer_auto_field_type(sample_values_by_field.get(f, []))

    sampled_summary = {pv: path for pv, path in sampled_partition_values.items() if pv}
    log_operation(
        op_id,
        "auto_schema_sample",
        "complete",
        result={
            "status": "success",
            "partition_field": partition_field,
            "sampled_partition_value_count": len(sampled_partition_values),
            "sampled_partition_values": sampled_summary,
        },
    )

    inferred_datetime_fields = [
        f for f, t in schema_types.items()
        if f != partition_field and (t or "").lower() in ("datetime", "timestamp")
    ]
    log_operation(
        op_id, "auto_schema_sample", "warning",
        result={
            "status": "warning",
            "message": (
                "Auto-schema mode: fields default to 'string' with safe datetime inference. "
                "Define an explicit output.schema for stricter typing control."
            ),
            "inferred_datetime_fields": inferred_datetime_fields,
        },
    )

    if unreadable_partition_values:
        log_operation(
            op_id, "auto_schema_sample", "warning",
            result={
                "status": "warning",
                "message": (
                    f"Could not read a parseable record from files for partition value(s): "
                    f"{unreadable_partition_values}."
                ),
                "unreadable_partition_values": unreadable_partition_values,
            },
        )

    return schema_field_names, schema_types, input_files_cache


def _write_output(
    op_id: str,
    start_time: float,
    output_format: str,
    all_rows: list,
    out_path: str,
    schema_field_names: List[str],
    schema_types: Dict[str, str],
    partition_by: List[str],
) -> str:
    """Write unified rows to Parquet or CSV and log the operation."""
    write_step = "write_csv" if output_format == "csv" else "write_parquet"
    log_operation(
        op_id, write_step, "start",
        inputs={"output_path": out_path, "row_count": len(all_rows), "partition_by": partition_by or None},
    )
    if output_format == "csv":
        write_table_csv(
            all_rows, out_path,
            schema_field_names=schema_field_names,
            schema_types=schema_types,
            partition_by=partition_by if partition_by else None,
        )
    else:
        write_table_parquet(
            all_rows, out_path,
            schema_types=schema_types,
            partition_by=partition_by if partition_by else None,
        )
    duration_write = int((time.time() - start_time) * 1000)
    log_operation(
        op_id, write_step, "complete",
        result={"status": "success", "output_path": out_path},
        duration=duration_write,
    )
    return out_path


def _run_passthrough_pipeline(
    op_id: str,
    storage_raw,
    config: dict,
    out_dir: str,
    pipeline_name: str,
) -> Tuple[List[str], str]:
    """
    Snapshot union mode: for each entity type, select the latest snapshot per site
    (newest y/m/d/h, all part-* files of that hour), union across all sites with
    pyarrow, add a ``siteLuid`` column, and write one fresh ``{entityType}.parquet``.
    A sidecar ``_manifest.json`` is written with the max snapshot_time across sites.

    Writes ``{out_dir}/_manifest.json`` with::

        {
          "users": {"snapshot_time": "2026-06-08T14:00:00Z", "filename": "users.parquet",
                    "source_paths": [...]},
          ...
        }

    The loader reads this manifest for replace-on-new watermarking.

    Per-entity union operations run in a thread pool for efficiency.
    pyarrow is required for this stage.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError(
            "pyarrow is required for the snapshot union pipeline. "
            "Install with: pip install pyarrow"
        )

    inputs = config.get("inputs") or []
    split_by_key = get_split_by(config) or get_partition_field(config)
    select_mode = get_select_mode(config)
    max_threads = 8

    # Collect all source files across inputs.
    all_files: List[str] = []
    for input_config in inputs:
        source = input_config.get("source") or {}
        folder_pattern = source.get("folder_pattern") or ""
        file_pattern = source.get("file_pattern") or "*.parquet"
        recursive = source.get("recursive", True)
        all_files.extend(
            discover_files_by_source(storage_raw, folder_pattern, file_pattern, recursive)
        )

    os.makedirs(out_dir, exist_ok=True)
    manifest: Dict[str, dict] = {}
    written_paths: List[str] = []
    errors: List[str] = []

    def _resolve_src(rel_path: str) -> str:
        if hasattr(storage_raw, "full_path"):
            return storage_raw.full_path(rel_path)
        return os.path.join(getattr(storage_raw, "base_path", ""), rel_path)

    if select_mode == "latest_per_partition":
        # Select each site's newest-hour part files per entity type.
        selected_by_entity = select_latest_per_site_per_partition(
            all_files, partition_key=split_by_key, site_key="siteLuid"
        )

        log_operation(op_id, "passthrough_select", "complete", result={
            "status": "success",
            "total_files": len(all_files),
            "partition_groups": len(selected_by_entity),
            "select_mode": select_mode,
            "split_by": split_by_key,
        })

        def _union_entity(pv_item):
            pv, (candidate_parts, max_date) = pv_item

            # Validate all candidate parts; collect valid ones.
            # If any part is corrupt, log a warning and skip it (the site-level
            # newest-hour was already selected; we do not fall back to an older
            # hour here because we may be mixing parts from different sites).
            valid_parts: List[str] = []
            for rel_path in candidate_parts:
                src_path = _resolve_src(rel_path)
                if not is_valid_parquet_file(src_path):
                    logger.warning(
                        "Snapshot integrity check failed for %r (%s=%s); skipping part.",
                        rel_path, split_by_key, pv,
                    )
                else:
                    valid_parts.append(rel_path)

            if not valid_parts:
                raise RuntimeError(
                    f"No valid (non-corrupt) Parquet snapshot parts found for "
                    f"{split_by_key}={pv!r}. All {len(candidate_parts)} candidate(s) "
                    f"failed integrity validation. Re-run the extractor to "
                    f"re-download the affected files."
                )

            # Read and union all valid parts, adding siteLuid column to each.
            tables: List["pa.Table"] = []
            for rel_path in valid_parts:
                src_path = _resolve_src(rel_path)
                try:
                    # Use ParquetFile.read() to read raw file content without
                    # Hive partition inference (which would add siteLuid, entityType,
                    # etc. columns automatically when the file lives inside key=value dirs).
                    tbl = pq.ParquetFile(src_path).read()
                except Exception as e:
                    logger.warning("Could not read %r: %s; skipping.", rel_path, e)
                    continue
                site_luid = extract_segment_value_from_path(rel_path, "siteLuid") or ""
                if "siteLuid" not in tbl.schema.names:
                    tbl = tbl.append_column(
                        pa.field("siteLuid", pa.string()),
                        pa.array([site_luid] * tbl.num_rows, type=pa.string()),
                    )
                tables.append(tbl)
                logger.debug("Snapshot union: reading %r (%s=%s)", rel_path, split_by_key, pv)

            if not tables:
                raise RuntimeError(
                    f"All Parquet parts for {split_by_key}={pv!r} were unreadable."
                )

            unified = pa.concat_tables(tables, promote_options="permissive")
            dest_filename = f"{pv}.parquet"
            dest_path = os.path.join(out_dir, dest_filename)
            tmp_dest = dest_path + ".part"
            try:
                pq.write_table(unified, tmp_dest)
                os.replace(tmp_dest, dest_path)
            except Exception:
                try:
                    os.remove(tmp_dest)
                except OSError:
                    pass
                raise
            logger.debug(
                "Snapshot union written: %r (%d rows from %d part(s))",
                dest_path, unified.num_rows, len(tables),
            )

            snapshot_time = _iso_from_date_tuple(max_date)
            return pv, {
                "snapshot_time": snapshot_time,
                "filename": dest_filename,
                "source_paths": valid_parts,
            }

        fatal_errors: List[str] = []
        with ThreadPoolExecutor(max_workers=max_threads) as pool:
            futures = {
                pool.submit(_union_entity, item): item[0]
                for item in selected_by_entity.items()
            }
            for future in as_completed(futures):
                pv = futures[future]
                try:
                    pv, entry = future.result()
                except RuntimeError as e:
                    logger.error("%s", e)
                    fatal_errors.append(str(pv))
                    errors.append(str(pv))
                    continue
                except Exception as e:
                    logger.error("Unexpected error processing %s=%r: %s", split_by_key, pv, e)
                    errors.append(str(pv))
                    continue
                if pv and entry:
                    manifest[pv] = entry
                    written_paths.append(os.path.join(out_dir, entry["filename"]))

        if fatal_errors:
            raise RuntimeError(
                f"Passthrough pipeline failed: {len(fatal_errors)} entity type(s) had no valid "
                f"Parquet snapshot ({', '.join(fatal_errors)}). "
                f"Re-run the extractor to re-download affected files."
            )

    else:
        # For 'all' and other modes: union all discovered files per entity type.
        # Each file gets a siteLuid column; files without a siteLuid get an empty string.
        selected = select_files_for_mapping(all_files, config)

        log_operation(op_id, "passthrough_select", "complete", result={
            "status": "success",
            "total_files": len(all_files),
            "selected_files": len(selected),
            "select_mode": select_mode,
            "split_by": split_by_key,
        })

        # Group selected files by partition value for union.
        by_entity: Dict[str, List[str]] = {}
        for rel_path in selected:
            pv = extract_segment_value_from_path(rel_path, split_by_key)
            if not pv:
                logger.warning(
                    "Cannot determine %s from path %r; skipping.", split_by_key, rel_path
                )
                continue
            by_entity.setdefault(pv, []).append(rel_path)

        def _union_entity_all(pv_files):
            pv, file_list = pv_files
            tables: List["pa.Table"] = []
            max_date: tuple = (0, 0, 0, 0)
            for rel_path in file_list:
                src_path = _resolve_src(rel_path)
                if not is_valid_parquet_file(src_path):
                    logger.warning(
                        "Snapshot integrity check failed for %r (%s=%s); skipping.",
                        rel_path, split_by_key, pv,
                    )
                    continue
                try:
                    tbl = pq.ParquetFile(src_path).read()
                except Exception as e:
                    logger.warning("Could not read %r: %s; skipping.", rel_path, e)
                    continue
                site_luid = extract_segment_value_from_path(rel_path, "siteLuid") or ""
                if "siteLuid" not in tbl.schema.names:
                    tbl = tbl.append_column(
                        pa.field("siteLuid", pa.string()),
                        pa.array([site_luid] * tbl.num_rows, type=pa.string()),
                    )
                tables.append(tbl)
                date_tup = _path_to_date_tuple(rel_path)
                if date_tup > max_date:
                    max_date = date_tup

            if not tables:
                logger.warning("No valid Parquet files for %s=%r; skipping.", split_by_key, pv)
                return None, None

            unified = pa.concat_tables(tables, promote_options="permissive")
            dest_filename = f"{pv}.parquet"
            dest_path = os.path.join(out_dir, dest_filename)
            tmp_dest = dest_path + ".part"
            try:
                pq.write_table(unified, tmp_dest)
                os.replace(tmp_dest, dest_path)
            except Exception:
                try:
                    os.remove(tmp_dest)
                except OSError:
                    pass
                raise
            snapshot_time = _iso_from_date_tuple(max_date)
            return pv, {
                "snapshot_time": snapshot_time,
                "filename": dest_filename,
                "source_paths": file_list,
            }

        with ThreadPoolExecutor(max_workers=max_threads) as pool:
            futures = {
                pool.submit(_union_entity_all, item): item[0]
                for item in by_entity.items()
            }
            for future in as_completed(futures):
                pv = futures[future]
                try:
                    pv, entry = future.result()
                except Exception as e:
                    logger.error("Unexpected error processing %s=%r: %s", split_by_key, pv, e)
                    errors.append(str(pv))
                    continue
                if pv and entry:
                    manifest[pv] = entry
                    written_paths.append(os.path.join(out_dir, entry["filename"]))

    # Write sidecar manifest atomically (temp + rename).
    manifest_path = os.path.join(out_dir, "_manifest.json")
    try:
        _write_json_atomic(manifest_path, manifest)
        written_paths.append(manifest_path)
        logger.debug("Snapshot manifest written: %s", manifest_path)
    except OSError as e:
        logger.error("Failed to write manifest %s: %s", manifest_path, e)

    if errors:
        log_operation(op_id, "passthrough_copy", "complete", result={
            "status": "degraded",
            "written_count": len(written_paths),
            "error_count": len(errors),
            "errors": errors,
        })
    else:
        log_operation(op_id, "passthrough_copy", "complete", result={
            "status": "success",
            "written_count": len(written_paths),
            "manifest_path": manifest_path,
        })

    return written_paths, "parquet"


def _normalize_pull_ts(pull_ts_fs: str) -> str:
    """Convert a filesystem-safe pulled= timestamp back to ISO 8601.

    e.g. "2026-06-15T08-30-00Z" → "2026-06-15T08:30:00Z"
    """
    if "T" in pull_ts_fs:
        date_part, time_part = pull_ts_fs.split("T", 1)
        time_part = time_part.replace("-", ":", 2)  # only first 2 hyphens are colons
        return f"{date_part}T{time_part}"
    return pull_ts_fs


def _find_newest_pulled_dir(storage_raw, entity_name: str) -> Tuple[Optional[str], List[str]]:
    """Return (newest_pulled_segment, sorted_page_file_list) for an entity, or (None, [])."""
    ref_prefix = f"reference/{entity_name}/"
    all_files = storage_raw.list_files(prefix=ref_prefix)
    pulled_dirs: Dict[str, List[str]] = {}
    for rel_path in all_files:
        parts = rel_path.replace("\\", "/").split("/")
        pulled_seg = next((p for p in parts if p.startswith("pulled=")), None)
        if pulled_seg:
            pulled_dirs.setdefault(pulled_seg, []).append(rel_path)
    if not pulled_dirs:
        return None, []
    newest = max(pulled_dirs.keys())
    return newest, sorted(pulled_dirs[newest])


def _load_pages(storage_raw, page_files: List[str], items_key: str, entity_name: str) -> List[dict]:
    """Load all page JSON files and extract the items list."""
    records: List[dict] = []
    for rel_path in page_files:
        if hasattr(storage_raw, "full_path"):
            full_path = storage_raw.full_path(rel_path)
        else:
            full_path = os.path.join(getattr(storage_raw, "base_path", ""), rel_path)
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                page_data = json.load(f)
        except Exception as e:
            logger.warning(
                "Reference pipeline '%s': could not read %s: %s; skipping page.",
                entity_name, rel_path, e,
            )
            continue
        records.extend(page_data.get(items_key) or [])
    return records


def _normalize_users(all_records: List[dict]) -> List[dict]:
    """Expand siteRoles[] into one row per (userLuid x siteLuid)."""
    rows: List[dict] = []
    for user in all_records:
        user_luid = user.get("userId") or user.get("userLuid") or ""
        site_roles = user.get("siteRoles") or []
        link_roles_raw = user.get("linkRoles") or []
        link_roles_str = json.dumps(link_roles_raw, ensure_ascii=False)
        base = {
            "userLuid": user_luid,
            "email": user.get("email"),
            "userName": user.get("userName"),
            "displayName": user.get("displayName"),
            "tenantId": user.get("tenantId"),
            "userStatus": user.get("userStatus"),
            "maxSiteRole": user.get("maxSiteRole"),
            "tcmRoleName": user.get("tcmRoleName"),
            "numberOfSiteRoles": user.get("numberOfSiteRoles"),
            "lastLoginTime": user.get("lastLoginTime"),
            "language": user.get("language"),
            "locale": user.get("locale"),
            "linkRoles": link_roles_str,
        }
        if site_roles:
            for sr in site_roles:
                row = dict(base)
                row["siteLuid"] = sr.get("siteId") or sr.get("siteLuid") or ""
                row["siteRole"] = sr.get("siteRole")
                row["idpType"] = sr.get("idpType")
                rows.append(row)
        else:
            row = dict(base)
            row["siteLuid"] = None
            row["siteRole"] = None
            row["idpType"] = None
            rows.append(row)
    return rows


def _normalize_sites(all_records: List[dict]) -> List[dict]:
    """Alias siteUUID → siteLuid and retain all other site attributes."""
    rows: List[dict] = []
    for site in all_records:
        row = {"siteLuid": site.get("siteUUID") or site.get("siteLuid") or ""}
        for k, v in site.items():
            if k not in ("siteUUID",):
                row.setdefault(k, v)
        rows.append(row)
    return rows


def _coerce_scalar(v):
    """JSON-serialize nested dicts/lists so every reference column is a Hyper-compatible scalar."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def _rows_to_parquet(rows: List[dict], out_path: str) -> None:
    """Write a list of flat dicts to Parquet.

    Any dict or list values are JSON-serialized to strings so the resulting
    Parquet contains only scalar columns that pantab/Hyper can load. This
    retains all attributes without narrowing (matches the linkRoles[] pattern).
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("pyarrow is required for reference normalization. pip install pyarrow")

    all_cols: List[str] = []
    for row in rows:
        for k in row:
            if k not in all_cols:
                all_cols.append(k)

    arrays = [pa.array([_coerce_scalar(row.get(col)) for row in rows]) for col in all_cols]
    table = pa.table(dict(zip(all_cols, arrays)))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pq.write_table(table, out_path)


def _run_reference_pipeline(
    op_id: str,
    storage_raw,
    config: dict,
    out_dir: str,
    pipeline_name: str,
) -> Tuple[List[str], str]:
    """
    Reference normalization pipeline for tenant-wide TCM REST data.

    Triggered when the mapping has ``reference_mode: true``.

    Reads the newest ``pulled=<TS>`` directory for each entity listed in
    ``config["reference_entities"]``, normalizes records, and writes:
      - ``{out_dir}/{table_name}.parquet`` per entity
      - ``{out_dir}/_manifest.json`` merged with any existing entries

    For ``tenant_users``: expands siteRoles[] to membership grain.
    For ``tenant_sites``: identity grain, siteUUID aliased to siteLuid.

    ``snapshot_time`` in the manifest = the ``pulled=<TS>`` directory
    timestamp (ISO 8601, colons restored from hyphens).
    """
    os.makedirs(out_dir, exist_ok=True)

    entity_specs = config.get("reference_entities") or []
    if not entity_specs:
        logger.warning("Reference pipeline: no reference_entities configured in mapping.")
        return [], "parquet"

    manifest_path = os.path.join(out_dir, "_manifest.json")
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest: dict = json.load(f)
    except (OSError, json.JSONDecodeError):
        manifest = {}

    all_written: List[str] = []

    for spec in entity_specs:
        entity_name = spec.get("name", "")
        table_name = spec.get("table_name") or entity_name
        items_key = spec.get("items_key", "users" if "user" in entity_name else "sites")

        if not entity_name:
            logger.warning("Reference pipeline: skipping spec with no 'name'.")
            continue

        newest_pulled, page_files = _find_newest_pulled_dir(storage_raw, entity_name)
        if newest_pulled is None:
            logger.warning(
                "Reference pipeline '%s': no pulled= directories found; skipping.", entity_name
            )
            continue

        pull_ts_fs = newest_pulled.replace("pulled=", "", 1)
        pull_ts_iso = _normalize_pull_ts(pull_ts_fs)

        logger.info(
            "Reference pipeline '%s': reading %d page file(s) from %s (snapshot_time=%s)",
            entity_name, len(page_files), newest_pulled, pull_ts_iso,
        )

        all_records = _load_pages(storage_raw, page_files, items_key, entity_name)
        logger.info("Reference pipeline '%s': %d raw records loaded.", entity_name, len(all_records))

        if not all_records:
            logger.warning("Reference pipeline '%s': no records to write.", entity_name)
            continue

        is_users = "user" in entity_name
        rows = _normalize_users(all_records) if is_users else _normalize_sites(all_records)
        grain = "membership" if is_users else "identity"
        logger.info(
            "Reference pipeline '%s': %d rows after normalization (grain: %s).",
            entity_name, len(rows), grain,
        )

        out_path = os.path.join(out_dir, f"{table_name}.parquet")
        _rows_to_parquet(rows, out_path)
        logger.info("Reference pipeline '%s': wrote %d rows to %s.", entity_name, len(rows), out_path)

        manifest[table_name] = {
            "snapshot_time": pull_ts_iso,
            "filename": f"{table_name}.parquet",
            "source_entity": entity_name,
            "pulled_dir": newest_pulled,
        }
        all_written.append(out_path)

    _write_json_atomic(manifest_path, manifest, ensure_ascii=False)

    logger.info(
        "Reference pipeline: complete. %d entity(ies) written, manifest at %s.",
        len(all_written), manifest_path,
    )
    return all_written, "parquet"


def run_pipeline(
    storage_raw,
    output_dir: str,
    mapping_path: str,
):
    """
    Run the transform pipeline for one mapping file.

    Routing logic:
      - ``reference_mode: true`` → reference normalization pipeline (JSON pages → Parquet).
      - ``passthrough: true`` → Parquet passthrough (copies + sidecar manifest); no JSON parsing.
      - ``output.schema: auto`` → auto-schema discovery, union all inputs.
      - explicit schema → field-mapped union.

    All paths respect:
      - ``partition_field`` (default "eventType"): the path segment key for partition values.
      - ``select`` mode (default "all"): file selection before union; see reader.select_files_for_mapping.
      - ``split_by``: per-partition output (combined with passthrough).
      - ``snapshot_time_field``: inject snapshotTimestamp column from path date block (non-passthrough).

    :param storage_raw: Storage backend for raw extract files.
    :param output_dir: Directory to write output files.
    :param mapping_path: Path to the YAML mapping file.
    :return: Tuple of (list of output file path(s), output_format string).
    """
    op_id = create_operation_id()
    op_name = "run_pipeline"
    start_time = time.time()

    log_operation(
        op_id, op_name, "start",
        inputs={"mapping_path": mapping_path, "output_dir": output_dir},
    )

    config = load_mapping_config(mapping_path)
    pipeline_name = config.get("output_filename") or "pipeline"
    output_cfg = config.get("output") or {}
    target_path = output_cfg.get("target_path") or ""
    output_format = (output_cfg.get("format") or "parquet").strip().lower()
    partition_by = output_cfg.get("partition_by") or []
    if isinstance(partition_by, str):
        partition_by = [partition_by]
    inputs = config.get("inputs") or []
    auto = is_auto_schema(config)
    partition_field = get_partition_field(config)
    snapshot_time_field = get_snapshot_time_field(config)

    log_operation(
        op_id, "load_mapping", "complete",
        result={
            "status": "success",
            "output_filename": pipeline_name,
            "schema_mode": "auto" if auto else "explicit",
            "passthrough": is_passthrough(config),
            "split_by": get_split_by(config),
            "partition_field": partition_field,
            "select": get_select_mode(config),
            "inputs_count": len(inputs),
            "output_format": output_format,
        },
    )

    # Resolve output directory.
    if target_path and os.path.isabs(target_path):
        out_dir = target_path
    elif target_path:
        out_dir = os.path.join(output_dir, target_path)
    else:
        out_dir = output_dir

    # -----------------------------------------------------------------------
    # Reference normalization mode (JSON pages → Parquet + manifest)
    # -----------------------------------------------------------------------
    if config.get("reference_mode"):
        written, fmt = _run_reference_pipeline(op_id, storage_raw, config, out_dir, pipeline_name)
        duration_ms = int((time.time() - start_time) * 1000)
        log_operation(
            op_id, op_name, "complete",
            result={"status": "success", "mode": "reference", "written_paths": written},
            duration=duration_ms,
        )
        return written, fmt

    # -----------------------------------------------------------------------
    # Passthrough mode (Parquet copy-through + sidecar manifest)
    # -----------------------------------------------------------------------
    if is_passthrough(config):
        written, fmt = _run_passthrough_pipeline(op_id, storage_raw, config, out_dir, pipeline_name)
        duration_ms = int((time.time() - start_time) * 1000)
        log_operation(
            op_id, op_name, "complete",
            result={
                "status": "success",
                "mode": "passthrough",
                "written_paths": written,
            },
            duration=duration_ms,
        )
        return written, fmt

    # -----------------------------------------------------------------------
    # Standard JSON-parsing pipeline (auto or explicit schema)
    # -----------------------------------------------------------------------
    ext = ".csv" if output_format == "csv" else ".parquet"
    out_path = os.path.join(out_dir, f"{pipeline_name}{ext}")
    if partition_by:
        out_path = os.path.join(out_dir, pipeline_name)

    # --- Auto-schema path ---
    if auto:
        schema_field_names, schema_types, input_files_cache = _sample_auto_schema(
            op_id, storage_raw, inputs, partition_field=partition_field,
        )
        # Optionally inject snapshot_time_field.
        if snapshot_time_field and snapshot_time_field not in schema_field_names:
            schema_field_names.append(snapshot_time_field)
            schema_types[snapshot_time_field] = "string"

        log_operation(
            op_id, "auto_schema_discover", "complete",
            result={
                "status": "success",
                "schema_field_count": len(schema_field_names),
                "schema_fields": schema_field_names,
            },
        )

        all_rows = []
        for i, input_config in enumerate(inputs):
            files = input_files_cache[i]
            # Apply file selection before processing.
            files = select_files_for_mapping(files, config)
            for rel_path in files:
                path_pv = extract_segment_value_from_path(rel_path, partition_field)
                effective_pv = (
                    path_pv
                    if path_pv
                    else (input_config.get(partition_field) or input_config.get("eventType") or "")
                )
                records = load_json_records(storage_raw, rel_path)
                date_tup = _path_to_date_tuple(rel_path)
                snap_time = _iso_from_date_tuple(date_tup) if snapshot_time_field else None
                for record in records:
                    row = apply_auto_mapping(
                        record, effective_pv, schema_field_names,
                        partition_field=partition_field,
                    )
                    if snapshot_time_field and snap_time is not None:
                        row[snapshot_time_field] = snap_time
                    all_rows.append(row)

    # --- Explicit schema path ---
    else:
        schema_field_names = get_schema_field_names(config)
        schema_types = get_schema_types(config)
        # Optionally inject snapshot_time_field into explicit schema.
        if snapshot_time_field and snapshot_time_field not in schema_field_names:
            schema_field_names.append(snapshot_time_field)
            schema_types[snapshot_time_field] = "string"

        log_operation(
            op_id, "load_mapping", "complete",
            result={"status": "success", "schema_field_count": len(schema_field_names)},
        )

        type_warnings = validate_mapping_types(config)
        for warning in type_warnings:
            log_operation(
                op_id, "validate_mapping", "warning",
                result={
                    "status": "warning",
                    "field": warning["field"],
                    "schema_type": warning["schema_type"],
                    "sources": warning["sources"],
                    "message": warning["message"],
                },
            )

        if not schema_field_names:
            duration_ms = int((time.time() - start_time) * 1000)
            log_operation(
                op_id, op_name, "complete",
                result={"status": "failure", "details": "No schema fields in mapping"},
                duration=duration_ms,
            )
            return [], output_format

        all_rows = []
        for input_config in inputs:
            files = _discover_input_files(op_id, storage_raw, input_config)
            files = select_files_for_mapping(files, config)
            for rel_path in files:
                path_pv = extract_segment_value_from_path(rel_path, partition_field)
                effective_pv = (
                    path_pv
                    if path_pv
                    else (input_config.get(partition_field) or input_config.get("eventType") or "")
                )
                records = load_json_records(storage_raw, rel_path)
                date_tup = _path_to_date_tuple(rel_path)
                snap_time = _iso_from_date_tuple(date_tup) if snapshot_time_field else None
                for record in records:
                    row = apply_input_mapping(
                        record, effective_pv, input_config, schema_field_names,
                        partition_field=partition_field,
                    )
                    if snapshot_time_field and snap_time is not None:
                        row[snapshot_time_field] = snap_time
                    all_rows.append(row)

    # Write unified output.
    _write_output(
        op_id, start_time, output_format, all_rows, out_path,
        schema_field_names, schema_types, partition_by,
    )

    duration_ms = int((time.time() - start_time) * 1000)
    log_operation(
        op_id, op_name, "complete",
        result={
            "status": "success",
            "output_filename": pipeline_name,
            "total_row_count": len(all_rows),
            "written_paths": [out_path],
        },
        duration=duration_ms,
    )
    return [out_path], output_format


def _looks_like_iso_datetime(value: object) -> bool:
    """Return True when value is an ISO-like datetime string parsable by fromisoformat."""
    if isinstance(value, datetime):
        return True
    if value is None:
        return False
    s = str(value).strip()
    if not s or "T" not in s:
        return False
    s = s.replace("Z", "+00:00")
    if "." in s:
        i = s.index(".")
        rest = s[i + 1:]
        plus_idx = rest.find("+")
        minus_idx = rest.find("-")
        end = len(rest)
        if plus_idx != -1:
            end = min(end, plus_idx)
        if minus_idx != -1:
            end = min(end, minus_idx)
        frac = rest[:end]
        if len(frac) > 6:
            s = s[: i + 1] + frac[:6] + rest[end:]
    try:
        datetime.fromisoformat(s)
        return True
    except (ValueError, TypeError):
        return False


def _infer_auto_field_type(samples: List[object]) -> str:
    """
    Infer field type for auto-schema.
    - datetime when all non-empty sampled values parse as ISO datetime.
    - otherwise string (safe default for heterogeneous event/snapshot unions).
    """
    non_empty = [v for v in samples if v is not None and not (isinstance(v, str) and not v.strip())]
    if not non_empty:
        return "string"
    if all(_looks_like_iso_datetime(v) for v in non_empty):
        return "datetime"
    return "string"
