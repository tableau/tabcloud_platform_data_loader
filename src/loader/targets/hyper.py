"""
Hyper target: write Parquet to local Hyper file via Pantab, list tables for destination detection.

We pass a PyArrow Table to pantab's frame_to_hyper (per Bring your own DataFrame).
Type normalization is done in Arrow space via normalize_table_for_hyper; we do not use
pandas dtypes for compatibility. See: https://pantab.readthedocs.io/en/latest/common_issues.html
"""

import os
from typing import Any, List, Optional

from loader.logging_utils import get_logger

logger = get_logger("hyper")

# pantab for Hyper I/O; pyarrow required for Table
_pantab_import_error: Optional[Exception] = None
try:
    import pantab as pt
except Exception as e:
    pt = None  # type: ignore
    _pantab_import_error = e
try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pc = None  # type: ignore
    pq = None  # type: ignore

# Default schema and table name used by Tableau
DEFAULT_SCHEMA = "Extract"
DEFAULT_TABLE = "Extract"


def _check_deps() -> None:
    if pt is None:
        if _pantab_import_error is not None:
            raise ImportError(
                "pantab failed to import (Hyper target). Install with: pip install pantab. "
                "Ensure you use the same Python that runs this script (e.g. python -m pip install pantab). "
                "Original error: %s" % _pantab_import_error
            ) from _pantab_import_error
        raise ImportError("pantab is required for Hyper target. Install with: pip install pantab")
    if pa is None:
        raise ImportError("pyarrow is required for Hyper target. Install with: pip install pyarrow")


def _target_arrow_type(field: "pa.Field") -> "pa.DataType":
    """Return the Arrow type to cast to for Hyper (string, int64, float64, timestamp[us], binary)."""
    t = field.type
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return pa.string()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return pa.binary()
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t) or pa.types.is_int64(t):
        return pa.int64()
    if pa.types.is_uint8(t) or pa.types.is_uint16(t) or pa.types.is_uint32(t) or pa.types.is_uint64(t):
        return pa.int64()
    if pa.types.is_floating(t):
        return pa.float64()
    if pa.types.is_timestamp(t) or pa.types.is_date32(t) or pa.types.is_date64(t):
        return pa.timestamp("us", tz="UTC")
    if pa.types.is_boolean(t):
        return pa.bool_()
    # Fallback: string (e.g. dictionary, list, struct)
    return pa.string()


def normalize_table_for_hyper(table: "pa.Table") -> "pa.Table":
    """
    Cast each column to Arrow types that pantab/Hyper expect. No pandas dtypes.
    Returns a new Table with columns cast to string, int64, float64, timestamp[us], binary, or bool.
    """
    if table is None or table.num_rows == 0:
        return table
    columns = []
    for i in range(table.num_columns):
        col = table.column(i)
        field = table.schema.field(i)
        target = _target_arrow_type(field)
        if field.type == target:
            columns.append(col)
        else:
            try:
                # Allow timestamp precision truncation (e.g. ns -> us) for Hyper
                if pa.types.is_timestamp(target):
                    cast_opts = pc.CastOptions(target_type=target, allow_time_truncate=True)
                    columns.append(pc.cast(col, options=cast_opts))
                else:
                    columns.append(pc.cast(col, target))
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                columns.append(pc.cast(col, pa.string()))
    return pa.table(columns, names=table.schema.names)


def hyper_file_exists(hyper_path: str) -> bool:
    """Return True if the Hyper file exists and is a file."""
    return os.path.isfile(hyper_path)


def list_hyper_tables(hyper_path: str) -> List[str]:
    """
    List table names in a Hyper file without loading table data.
    Uses a lightweight catalog query.

    :param hyper_path: Path to .hyper file.
    :return: List of table names (schema.table or table).
    """
    _check_deps()
    if not hyper_file_exists(hyper_path):
        return []
    try:
        tbl = pt.frame_from_hyper_query(
            hyper_path,
            "SELECT table_schema, table_name FROM information_schema.tables",
            return_type="pyarrow",
        )
        if tbl is None or tbl.num_rows == 0:
            return _list_hyper_tables_fallback(hyper_path)
        out = []
        if "table_schema" not in tbl.schema.names or "table_name" not in tbl.schema.names:
            return _list_hyper_tables_fallback(hyper_path)
        schema_vals = tbl.column("table_schema").to_pylist()
        name_vals = tbl.column("table_name").to_pylist()
        for schema, name in zip(schema_vals, name_vals):
            schema = "" if schema is None else str(schema)
            name = "" if name is None else str(name)
            if schema and schema not in ("information_schema", "hyper") and name:
                out.append(name if schema == "Extract" else f"{schema}.{name}")
        return out if out else _list_hyper_tables_fallback(hyper_path)
    except Exception:
        return _list_hyper_tables_fallback(hyper_path)


def _list_hyper_tables_fallback(hyper_path: str) -> List[str]:
    """Fallback: use frames_from_hyper and take keys (loads metadata only in some versions)."""
    try:
        frames = pt.frames_from_hyper(hyper_path, return_type="pyarrow")
        if not isinstance(frames, dict):
            return []
        result = []
        for key in frames.keys():
            if isinstance(key, tuple) and len(key) == 2:
                schema, name = str(key[0] or ""), str(key[1] or "")
                if schema and schema not in ("information_schema", "hyper"):
                    result.append(name if schema == "Extract" else f"{schema}.{name}")
            elif key:
                result.append(str(key))
        return result
    except Exception:
        return []


def read_hyper_table(hyper_path: str, table: str = DEFAULT_TABLE):
    """
    Read a single table from a Hyper file as a pandas DataFrame.

    :param hyper_path: Path to .hyper file.
    :param table: Table name (default Extract).
    :return: pandas DataFrame.
    """
    _check_deps()
    return pt.frame_from_hyper(hyper_path, table=table, return_type="pandas")


def write_parquet_to_hyper(
    parquet_path: str,
    hyper_path: str,
    table: str = DEFAULT_TABLE,
    table_mode: str = "w",
    schema: str = DEFAULT_SCHEMA,
) -> int:
    """
    Write a single Parquet file to a Hyper file (one table).
    Reads with PyArrow and passes a normalized Table to pantab (no pandas dtypes).
    """
    _check_deps()
    tbl = pq.read_table(parquet_path)
    tbl = normalize_table_for_hyper(tbl)
    n = tbl.num_rows
    os.makedirs(os.path.dirname(hyper_path) or ".", exist_ok=True)
    pt.frame_to_hyper(tbl, hyper_path, table=(schema, table), table_mode=table_mode)
    return n


def write_table_to_hyper(
    table: "pa.Table",
    hyper_path: str,
    table_name: str = DEFAULT_TABLE,
    table_mode: str = "w",
    schema: str = DEFAULT_SCHEMA,
) -> int:
    """
    Write a PyArrow Table to a Hyper file.
    Normalizes types in Arrow space and passes Table to pantab (no pandas).
    """
    _check_deps()
    tbl = normalize_table_for_hyper(table)
    n = tbl.num_rows
    os.makedirs(os.path.dirname(hyper_path) or ".", exist_ok=True)
    pt.frame_to_hyper(tbl, hyper_path, table=(schema, table_name), table_mode=table_mode)
    return n


def get_hyper_state(hyper_path: str) -> Optional[dict]:
    """
    Get state for Hyper file target: exists, list of table names.

    :param hyper_path: Path to .hyper file.
    :return: {"exists": bool, "tables": [...]} or None if path empty.
    """
    if not (hyper_path or "").strip():
        return None
    return {
        "exists": hyper_file_exists(hyper_path),
        "tables": list_hyper_tables(hyper_path) if hyper_file_exists(hyper_path) else [],
    }


def read_hyper_table_max(
    hyper_path: str,
    table: str,
    column: str,
    schema: str = DEFAULT_SCHEMA,
) -> Optional[Any]:
    """
    Query the Hyper file for the maximum value of a column in a table.
    Used as the high-water mark for incremental deduplication so the Hyper file
    itself — not a separate state sidecar — is the source of truth.

    Returns None if the file does not exist, the table is missing, or the column
    contains only NULLs (treated as first-run: load all rows).

    :param hyper_path: Path to .hyper file.
    :param table: Table name (e.g. "Extract").
    :param column: Column name to compute MAX over (e.g. "event_time").
    :param schema: Hyper schema name (default "Extract").
    :return: Python native max value (datetime, int, float, str), or None.
    """
    _check_deps()
    if not hyper_file_exists(hyper_path):
        return None
    try:
        result = pt.frame_from_hyper_query(
            hyper_path,
            f'SELECT MAX("{column}") AS max_val FROM "{schema}"."{table}"',
            return_type="pyarrow",
        )
        if result is None or result.num_rows == 0:
            return None
        scalar = result.column("max_val")[0]
        if not scalar.is_valid:
            return None
        return scalar.as_py()
    except Exception as e:
        logger.warning(
            "read_hyper_table_max failed for %s (table=%s, column=%s): %s",
            hyper_path, table, column, e,
        )
        return None


def get_hyper_table_columns(
    hyper_path: str,
    schema: str = DEFAULT_SCHEMA,
    table: str = DEFAULT_TABLE,
) -> Optional[List[Any]]:
    """
    Return the column definitions of an existing Hyper table as a list of
    ``(column_name, pa.DataType)`` tuples, or ``None`` if the file or table
    does not exist.

    Uses a zero-row probe (``SELECT * … LIMIT 0``) so no data is transferred.

    :param hyper_path: Path to .hyper file.
    :param schema: Hyper schema name (default "Extract").
    :param table: Table name (default "Extract").
    :return: List of (name, pa.DataType) or None.
    """
    _check_deps()
    if not hyper_file_exists(hyper_path):
        return None
    try:
        result = pt.frame_from_hyper_query(
            hyper_path,
            f'SELECT * FROM "{schema}"."{table}" LIMIT 0',
            return_type="pyarrow",
        )
        if result is None:
            return None
        return [(field.name, field.type) for field in result.schema]
    except Exception:
        return None


def diff_hyper_schema(
    existing_cols: List[Any],
    incoming_table: "pa.Table",
) -> Optional[dict]:
    """
    Compare an existing Hyper table's columns against an incoming PyArrow table.

    Comparison is done after applying the same type normalisation used before
    writing (via :func:`normalize_table_for_hyper`), so cosmetic type differences
    (e.g. int32 vs int64 that both normalise to int64) are not reported as drift.

    Returns ``None`` when schemas match; otherwise returns a dict::

        {
          "added":        ["col_a", ...],   # in incoming, not in existing
          "removed":      ["col_b", ...],   # in existing, not in incoming
          "type_changed": [("col_c", existing_type, incoming_type), ...],
        }

    An empty ``None`` means no drift; a non-None dict (even with empty lists
    for all keys) means drift was detected.

    :param existing_cols: List of (name, pa.DataType) from :func:`get_hyper_table_columns`.
    :param incoming_table: PyArrow Table about to be written.
    :return: None if schemas match, else diff dict.
    """
    _check_deps()
    # Normalise the incoming table so we compare the actual types that would be written.
    normalised = normalize_table_for_hyper(incoming_table)
    existing: dict = {name: dtype for name, dtype in existing_cols}
    incoming: dict = {field.name: field.type for field in normalised.schema}

    existing_names = set(existing)
    incoming_names = set(incoming)

    added = sorted(incoming_names - existing_names)
    removed = sorted(existing_names - incoming_names)
    type_changed = []
    for col in sorted(existing_names & incoming_names):
        e_type = existing[col]
        i_type = incoming[col]
        if str(e_type) != str(i_type):
            type_changed.append((col, e_type, i_type))

    if not added and not removed and not type_changed:
        return None
    return {"added": added, "removed": removed, "type_changed": type_changed}


def drop_hyper_table(
    hyper_path: str,
    schema: str = DEFAULT_SCHEMA,
    table: str = DEFAULT_TABLE,
) -> bool:
    """
    Drop a single table from an existing Hyper file using ``tableauhyperapi``.

    Returns ``True`` if the table was dropped (or did not exist), ``False`` if
    ``tableauhyperapi`` is unavailable or an unexpected error occurs.

    :param hyper_path: Path to .hyper file.
    :param schema: Hyper schema name (default "Extract").
    :param table: Table name to drop (default "Extract").
    :return: True on success/no-op, False on failure.
    """
    if not hyper_file_exists(hyper_path):
        return True
    try:
        from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName
    except ImportError:
        logger.error(
            "tableauhyperapi not available; cannot drop table %s.%s from %s. "
            "Install with: pip install tableauhyperapi",
            schema, table, hyper_path,
        )
        return False
    try:
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(hyper.endpoint, hyper_path) as conn:
                conn.execute_command(
                    f'DROP TABLE IF EXISTS "{schema}"."{table}"'
                )
        return True
    except Exception as e:
        logger.warning("drop_hyper_table failed for %s.%s in %s: %s", schema, table, hyper_path, e)
        return False
