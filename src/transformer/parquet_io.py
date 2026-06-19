"""
Write tabular data (list of dicts) to Parquet files, with optional schema types and partitioning.
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# PyArrow type name -> pa type (lazy so pyarrow is optional until write)
_ARROW_TYPES = None


def _parse_iso_timestamp(value: Any) -> Optional[datetime]:
    """Parse ISO 8601 timestamp string (e.g. 2026-03-10T00:41:38.499721141Z) to datetime in UTC.
    Truncates to microsecond precision. Returns None for None or empty.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip().replace("Z", "+00:00")
    # Truncate fractional seconds to 6 digits (microseconds) for fromisoformat
    if "." in s:
        i = s.index(".")
        rest = s[i + 1:]
        end = rest.find("+") if "+" in rest else len(rest)
        frac = rest[:end]
        if len(frac) > 6:
            s = s[: i + 1] + frac[:6] + rest[end:]
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _coerce_value(value: Any, type_name: str) -> Any:
    """Coerce a Python value to the target type so that all values in a column share one Python
    type before PyArrow infers the schema.  Returns None for None/empty.
    Timestamps are left as-is (handled separately by _parse_iso_timestamp during cast).
    """
    if value is None:
        return None
    name = (type_name or "string").lower()
    if name in ("string", "str"):
        return str(value)
    if name in ("int", "int64", "integer"):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    if name in ("float", "float64", "double"):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    if name in ("bool", "boolean"):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ("true", "1", "yes"):
                return True
            if value.lower() in ("false", "0", "no"):
                return False
            return None
        try:
            return bool(value)
        except (ValueError, TypeError):
            return None
    # timestamp/datetime — leave as-is for _parse_iso_timestamp
    return value


def normalize_rows(
    rows: List[Dict[str, Any]],
    schema_types: Dict[str, str],
) -> None:
    """Coerce row values in-place so every column has a consistent Python type.
    Must be called before pa.Table.from_pylist to avoid ArrowInvalid when the same
    field arrives as different Python types across event sources (e.g. int vs str).
    """
    if not schema_types:
        return
    for row in rows:
        for field, type_name in schema_types.items():
            if field in row:
                row[field] = _coerce_value(row[field], type_name)


def _get_pa_type(type_name: str):
    try:
        import pyarrow as pa
    except ImportError:
        raise ImportError("pyarrow is required for Parquet output. Install with: pip install pyarrow")
    name = (type_name or "string").lower()
    if name in ("string", "str"):
        return pa.string()
    if name in ("timestamp", "datetime", "datetime64"):
        return pa.timestamp("us", tz="UTC")
    if name in ("int", "int64", "integer"):
        return pa.int64()
    if name in ("float", "float64", "double"):
        return pa.float64()
    if name == "bool":
        return pa.bool_()
    return pa.string()


def write_table_parquet(
    rows: List[Dict[str, Any]],
    output_path: str,
    schema_types: Optional[Dict[str, str]] = None,
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Write a list of row dicts to Parquet (single file or partitioned dataset).

    :param rows: List of dicts with consistent keys (column names)
    :param output_path: Full path for the output .parquet file (or directory if partition_by is set)
    :param schema_types: Optional map of field name -> type string (e.g. "string", "timestamp")
    :param partition_by: If set, write as partitioned dataset to output_path dir; else single file
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("pyarrow is required for Parquet output. Install with: pip install pyarrow")

    schema_types = schema_types or {}
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if not rows:
        if partition_by:
            # Empty partitioned write: still create directory
            if not os.path.isdir(output_path):
                os.makedirs(output_path, exist_ok=True)
            return
        table = pa.table({})
        pq.write_table(table, output_path)
        return

    normalize_rows(rows, schema_types)

    table = pa.Table.from_pylist(rows)
    if schema_types:
        # Build schema with requested types; parse timestamp columns explicitly (PyArrow cast
        # does not accept ISO strings with Z or nanosecond fractional seconds)
        timestamp_type = pa.timestamp("us", tz="UTC")
        new_columns = []
        for name in table.schema.names:
            pa_type = _get_pa_type(schema_types.get(name, "string"))
            col = table.column(name)
            if pa_type == timestamp_type:
                parsed = [_parse_iso_timestamp(col[i].as_py()) for i in range(col.length())]
                new_columns.append(pa.array(parsed, type=timestamp_type))
            else:
                new_columns.append(col.cast(pa_type))
        schema = pa.schema([pa.field(n, _get_pa_type(schema_types.get(n, "string"))) for n in table.schema.names])
        table = pa.table(dict(zip(table.schema.names, new_columns)), schema=schema)

    if partition_by:
        # Write partitioned dataset: output_path is the root directory.
        # Partitioned datasets are written into a dedicated directory so there is
        # no single file to atomically rename; overwrite_or_ignore is the best we
        # can do here (each part file is small and rewritten independently).
        partition_cols = partition_by if isinstance(partition_by, list) else [partition_by]
        base_dir = output_path.rstrip("/\\")
        if base_dir.endswith(".parquet"):
            base_dir = os.path.dirname(base_dir)
        pq.write_to_dataset(
            table, base_dir, partition_cols=partition_cols, existing_data_behavior="overwrite_or_ignore"
        )
    else:
        # Atomic write: write to a .part sibling then rename so a partial file is
        # never visible at the final path (e.g. on process kill or mid-write error).
        tmp_path = output_path + ".part"
        try:
            pq.write_table(table, tmp_path)
            os.replace(tmp_path, output_path)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise
