"""
Incremental load: insert-only by increment field (min/max date range).
No update/delete; we either replace a table fully or append rows where increment_field > last_max.
Uses PyArrow tables throughout.
"""

from datetime import datetime, timezone
from typing import Any, Optional, Tuple

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pc = None  # type: ignore
    pq = None  # type: ignore

from loader.logging_utils import get_logger

logger = get_logger("diff")


def _parse_iso_timestamp(value: Any) -> Optional[datetime]:
    """Parse JSON-loaded value (ISO string or number) to datetime in UTC for Arrow scalar."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # Unix timestamp (seconds or milliseconds)
        if value > 1e12:
            value = value / 1000.0
        return datetime.fromtimestamp(value, tz=timezone.utc)
    s = str(value).strip().replace("Z", "+00:00")
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


def _last_max_to_scalar(value: Any, field: "pa.Field") -> Optional["pa.Scalar"]:
    """
    Convert JSON-loaded last_max (str, int, float) to a PyArrow scalar for comparison.
    """
    if pa is None or value is None:
        return None
    t = field.type
    if pa.types.is_timestamp(t) or pa.types.is_date32(t) or pa.types.is_date64(t):
        dt = _parse_iso_timestamp(value)
        if dt is None:
            return None
        target = pa.timestamp("us", tz="UTC")
        return pa.scalar(dt, type=target)
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t) or pa.types.is_int64(t):
        return pa.scalar(int(value), type=pa.int64())
    if pa.types.is_uint8(t) or pa.types.is_uint16(t) or pa.types.is_uint32(t) or pa.types.is_uint64(t):
        return pa.scalar(int(value), type=pa.int64())
    if pa.types.is_floating(t):
        return pa.scalar(float(value), type=pa.float64())
    return pa.scalar(str(value), type=pa.string())


def get_new_rows_and_max(
    parquet_path: str,
    increment_field: str,
    last_max: Optional[object],
) -> Tuple["pa.Table", Optional[object]]:
    """
    From Parquet, select rows that are "new" for incremental load and the new max value.

    Rows are new when increment_field > last_max. If last_max is None (first run), all rows
    are considered (caller should replace the table; we still return new_max for state).

    :param parquet_path: Path to the Parquet file (full upstream data).
    :param increment_field: Column name used for ordering (e.g. event_time, date).
    :param last_max: Last loaded max value for increment_field; None if no previous load.
    :return: (Table of new rows to insert, new max value of increment_field in Parquet).
    """
    if pa is None or pc is None:
        raise ImportError("pyarrow is required for incremental load. Install with: pip install pyarrow")
    table = pq.read_table(parquet_path)
    if table.num_rows == 0 or increment_field not in table.schema.names:
        logger.debug("Parquet empty or missing column %s", increment_field)
        return (pa.table({}), None)
    col = table.column(increment_field)
    new_max_scalar = pc.max(col)
    new_max = new_max_scalar.as_py() if new_max_scalar.is_valid else None
    if last_max is None:
        logger.debug("No last_max; treating full Parquet as new (rows=%d)", table.num_rows)
        return (table, new_max)
    field = table.schema.field(table.schema.get_field_index(increment_field))
    scalar = _last_max_to_scalar(last_max, field)
    if scalar is None:
        logger.debug("Could not convert last_max to scalar; treating full Parquet as new")
        return (table, new_max)
    mask = pc.greater(col, scalar)
    new_rows = table.filter(mask)
    logger.debug(
        "Increment field %s: last_max=%s, new_max=%s, new rows=%d",
        increment_field, last_max, new_max, new_rows.num_rows,
    )
    return (new_rows, new_max)


def get_new_rows_and_max_table(
    table: "pa.Table",
    increment_field: str,
    last_max: Optional[object],
) -> Tuple["pa.Table", Optional[object]]:
    """
    From a PyArrow Table, select rows that are "new" for incremental load and the new max value.
    Same logic as get_new_rows_and_max but for an in-memory table (e.g. from source_file_path).

    :param table: Full upstream data.
    :param increment_field: Column name used for ordering.
    :param last_max: Last loaded max value; None if no previous load.
    :return: (Table of new rows to insert, new max value of increment_field).
    """
    if pa is None or pc is None:
        raise ImportError("pyarrow is required for incremental load. Install with: pip install pyarrow")
    if table.num_rows == 0 or increment_field not in table.schema.names:
        logger.debug("Table empty or missing column %s", increment_field)
        return (pa.table({}), None)
    col = table.column(increment_field)
    new_max_scalar = pc.max(col)
    new_max = new_max_scalar.as_py() if new_max_scalar.is_valid else None
    if last_max is None:
        return (table, new_max)
    field = table.schema.field(table.schema.get_field_index(increment_field))
    scalar = _last_max_to_scalar(last_max, field)
    if scalar is None:
        logger.debug("Could not convert last_max to scalar; treating full table as new")
        return (table, new_max)
    mask = pc.greater(col, scalar)
    new_rows = table.filter(mask)
    logger.debug(
        "Increment field %s: last_max=%s, new_max=%s, new rows=%d",
        increment_field, last_max, new_max, new_rows.num_rows,
    )
    return (new_rows, new_max)
