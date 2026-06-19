"""
Write tabular data (list of dicts) to CSV files, with optional schema types and partitioning.
Uses the same schema_field_names order and schema_types as the Parquet path for consistency.
"""

import csv
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from transformer.parquet_io import normalize_rows


def _format_cell(value: Any, type_name: str) -> str:
    """Format a single cell value for CSV output. Datetimes as ISO 8601; None as empty string."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    type_lower = (type_name or "string").lower()
    if type_lower in ("timestamp", "datetime", "datetime64") and isinstance(value, str):
        # Leave already-string timestamps as-is for CSV
        return value
    return str(value)


def write_table_csv(
    rows: List[Dict[str, Any]],
    output_path: str,
    schema_field_names: List[str],
    schema_types: Optional[Dict[str, str]] = None,
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Write a list of row dicts to CSV (single file or partitioned set of CSV files).

    :param rows: List of dicts with keys matching schema_field_names
    :param output_path: Full path for the output .csv file (or directory if partition_by is set)
    :param schema_field_names: Column order and names for header and row order
    :param schema_types: Optional map of field name -> type string (used for datetime formatting)
    :param partition_by: If set, write one CSV per partition under output_path dir; else single file
    """
    schema_types = schema_types or {}
    normalize_rows(rows, schema_types)
    if partition_by:
        partition_cols = partition_by if isinstance(partition_by, list) else [partition_by]
        base_dir = output_path.rstrip("/\\")
        if base_dir.endswith(".csv"):
            base_dir = os.path.dirname(base_dir)
        os.makedirs(base_dir, exist_ok=True)
        if not rows:
            return
        # Group rows by partition key (tuple of partition column values)
        groups = defaultdict(list)
        for row in rows:
            key_parts = []
            for col in partition_cols:
                val = row.get(col)
                key_parts.append("" if val is None else str(val))
            key = tuple(key_parts)
            groups[key].append(row)
        for key, group_rows in groups.items():
            part_dir = os.path.join(base_dir, *(f"{col}={val}" for col, val in zip(partition_cols, key)))
            os.makedirs(part_dir, exist_ok=True)
            single_path = os.path.join(part_dir, "data.csv")
            _write_csv_file(group_rows, single_path, schema_field_names, schema_types)
    else:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        _write_csv_file_atomic(rows, output_path, schema_field_names, schema_types)


def _write_csv_file(
    rows: List[Dict[str, Any]],
    path: str,
    schema_field_names: List[str],
    schema_types: Dict[str, str],
) -> None:
    """Write a single CSV file with header and ordered columns (non-atomic; used for partitioned sets)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(schema_field_names)
        for row in rows:
            writer.writerow(
                _format_cell(row.get(name), schema_types.get(name, "string"))
                for name in schema_field_names
            )


def _write_csv_file_atomic(
    rows: List[Dict[str, Any]],
    path: str,
    schema_field_names: List[str],
    schema_types: Dict[str, str],
) -> None:
    """Write a single CSV file atomically (temp write then rename)."""
    tmp_path = path + ".part"
    try:
        _write_csv_file(rows, tmp_path, schema_field_names, schema_types)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise
