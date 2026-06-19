"""
Load mapping YAML and apply per-input field mappings to records (unified pipeline format).

New snapshot-related mapping keys (all optional; ignored for activity-log mappings):

  partition_field: entityType   # which path segment key carries the "type" label
                                # (default: "eventType")

  select: latest_per_partition  # file selection mode before union:
                                #   "all" (default for events) — include every file
                                #   "latest_per_partition" (default for snapshots) — keep only
                                #     the newest snapshot per partition value
                                #   "all_snapshots" — RESERVED, future SCD; not implemented yet.
                                #     Would union multiple snapshots of the same entity over time
                                #     with an effective-time column for slowly-changing-dimension
                                #     (Type 2) history tracking.  Stub point: logic would slot
                                #     into reader.select_files() after initial discovery.

  passthrough: true             # copy Parquet files directly without JSON parsing (fastest
                                # path for snapshot files that arrive already in Parquet format)

  split_by: entityType          # write one output file per partition-field value instead of
                                # a single unioned output; combined with passthrough this
                                # produces {output_dir}/{PartitionValue}.parquet per entity

  snapshot_time_field: snapshotTimestamp
                                # inject the snapshot time (from path y=/m=/d=/h= block) as
                                # this column name; also controls what the sidecar manifest uses.
                                # (only used without passthrough=true; in passthrough mode the
                                #  time is captured in the sidecar manifest instead)
"""

from typing import Any, Dict, List


def _get_nested(data: dict, key_path: str) -> Any:
    """Get value from dict by dotted key path (e.g. 'a.b.c'). Returns None if any key is missing."""
    keys = key_path.split(".")
    obj = data
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
        if obj is None:
            return None
    return obj


def load_mapping_config(path: str) -> dict:
    """
    Load a mapping YAML file (unified pipeline format). Requires PyYAML.

    Expected top-level keys: output_filename, output (schema, target_path, partition_by, format),
    inputs (list of { eventType, source: { base_path, file_pattern, recursive }, mappings }).

    :param path: Path to the YAML file
    :return: Parsed config
    """
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required for mapping configs. Install with: pip install pyyaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def apply_input_mapping(
    record: dict,
    partition_value: str,
    input_config: dict,
    schema_field_names: List[str],
    partition_field: str = "eventType",
) -> Dict[str, Any]:
    """
    Map a single JSON record to one row for the given input config using the master schema.

    Uses input_config['mappings']: each key is an output (schema) field name, value is either:
      - A JSON path (e.g. "user.id")
      - The literal ``$eventType`` (back-compat) or ``${partition_field}`` to inject the
        current partition value (e.g. "Login" for event mappings, "Users" for snapshots).

    :param record: Raw JSON record (flat or nested)
    :param partition_value: Partition value for this record (eventType or entityType from path)
    :param input_config: One element from mapping config 'inputs'
    :param schema_field_names: List of output field names from output.schema (order preserved)
    :param partition_field: The partition key name (default "eventType"); controls which
        literal specifier is treated as the injected partition value.
    :return: Dict of output_column -> value; missing schema fields are set to None
    """
    mappings = input_config.get("mappings") or {}
    # Both "$eventType" (back-compat) and "$<partition_field>" inject the partition value.
    partition_specifiers = {"$eventType", f"${partition_field}"}
    row = {}
    for output_name in schema_field_names:
        source_spec = mappings.get(output_name)
        if source_spec in partition_specifiers:
            value = partition_value
        elif source_spec is None or not isinstance(source_spec, str) or not source_spec.strip():
            value = None
        else:
            value = _get_nested(record, source_spec)
        row[output_name] = value
    return row


def is_auto_schema(config: dict) -> bool:
    """Return True if output.schema is set to the string 'auto' (schema discovery mode)."""
    schema_val = (config.get("output") or {}).get("schema")
    return isinstance(schema_val, str) and schema_val.strip().lower() == "auto"


def get_schema_field_names(config: dict) -> List[str]:
    """Return ordered list of field names from output.schema.
    Returns [] when schema is 'auto' (pipeline handles discovery)."""
    if is_auto_schema(config):
        return []
    schema = (config.get("output") or {}).get("schema") or []
    return [s.get("field") for s in schema if s.get("field")]


def get_schema_types(config: dict) -> Dict[str, str]:
    """Return map of field name -> type from output.schema (e.g. string, timestamp).
    Returns {} when schema is 'auto' (pipeline handles discovery)."""
    if is_auto_schema(config):
        return {}
    schema = (config.get("output") or {}).get("schema") or []
    return {s["field"]: (s.get("type") or "string") for s in schema if s.get("field")}


def apply_auto_mapping(
    record: dict,
    partition_value: str,
    schema_field_names: List[str],
    partition_field: str = "eventType",
) -> Dict[str, Any]:
    """
    Map a single JSON record to one row using auto-schema field names.

    Each schema field is mapped from the JSON record key of the same name.
    The ``partition_field`` column (default "eventType") is always populated from
    ``partition_value`` (derived from the file path) rather than the record itself.

    :param record: Raw JSON record
    :param partition_value: Partition value for this record (from path segment)
    :param schema_field_names: Ordered list of discovered schema field names
    :param partition_field: Which field name carries the partition value (default "eventType").
    :return: Dict of field_name -> value; fields absent in the record become None
    """
    row = {}
    for field in schema_field_names:
        if field == partition_field:
            row[field] = partition_value
        else:
            row[field] = _get_nested(record, field)
    return row


def get_partition_field(config: dict) -> str:
    """Return the partition_field for this mapping config (default 'eventType')."""
    return str((config.get("partition_field") or "eventType")).strip() or "eventType"


def get_select_mode(config: dict) -> str:
    """Return the file-selection mode for this mapping config.

    Values:
      "all"                   — include every discovered file (default for event mappings).
      "latest_per_partition"  — keep only the newest snapshot per partition value (default
                                for snapshot mappings when passthrough=True is set).
      "all_snapshots"         — RESERVED for future SCD / history (not implemented).
    """
    return str((config.get("select") or "all")).strip().lower() or "all"


def is_passthrough(config: dict) -> bool:
    """Return True when the mapping requests Parquet passthrough (no JSON re-parsing)."""
    return bool(config.get("passthrough"))


def get_split_by(config: dict) -> str:
    """Return the split_by field (e.g. 'entityType'), or '' if not set."""
    return str((config.get("split_by") or "")).strip()


def get_snapshot_time_field(config: dict) -> str:
    """Return the snapshot_time_field name (e.g. 'snapshotTimestamp'), or '' if not set."""
    return str((config.get("snapshot_time_field") or "")).strip()


_STRING_TYPES = frozenset(("string", "str"))


def validate_mapping_types(config: dict) -> List[Dict[str, Any]]:
    """Validate a mapping config for potential type conflicts when multiple inputs
    map to the same output schema.

    Returns a list of warning dicts, each with:
      - field: the output schema field name
      - schema_type: the declared output type
      - sources: dict of eventType -> source field spec
      - message: human-readable explanation
    """
    schema_types = get_schema_types(config)
    inputs = config.get("inputs") or []
    if len(inputs) < 2:
        return []

    warnings: List[Dict[str, Any]] = []

    for field, schema_type in schema_types.items():
        type_lower = (schema_type or "string").lower()
        sources: Dict[str, str] = {}
        for inp in inputs:
            event_type = inp.get("eventType") or "unknown"
            mappings = inp.get("mappings") or {}
            source_spec = mappings.get(field)
            if source_spec is None:
                sources[event_type] = "<unmapped>"
            elif not isinstance(source_spec, str) or not source_spec.strip():
                sources[event_type] = "<null>"
            else:
                sources[event_type] = source_spec

        unique_real_sources = {
            s for s in sources.values() if s not in ("<unmapped>", "<null>", "$eventType")
        }

        if len(unique_real_sources) > 1 and type_lower not in _STRING_TYPES:
            warnings.append({
                "field": field,
                "schema_type": schema_type,
                "sources": dict(sources),
                "message": (
                    f"Field '{field}' (type: {schema_type}) is mapped from different "
                    f"source fields across inputs: {sources}. Different source fields "
                    f"may have incompatible JSON types. Consider using 'string' as the "
                    f"schema type for safe union, or verify that source types are compatible."
                ),
            })
        elif len(unique_real_sources) == 1 and type_lower not in _STRING_TYPES:
            source_name = next(iter(unique_real_sources))
            has_null = any(s in ("<unmapped>", "<null>") for s in sources.values())
            if not has_null:
                # Same source field across all inputs with a non-string type.
                # Heterogeneous APIs may still deliver different JSON types for the
                # same field name (e.g. siteId as int in one event, str in another).
                if type_lower in ("int", "int64", "integer", "float", "float64", "double"):
                    warnings.append({
                        "field": field,
                        "schema_type": schema_type,
                        "sources": dict(sources),
                        "message": (
                            f"Field '{field}' (type: {schema_type}) maps to '{source_name}' "
                            f"in all inputs. Note: heterogeneous source APIs may deliver "
                            f"different JSON types for the same field name (e.g. integer in "
                            f"one event type, string in another). Values will be coerced to "
                            f"{schema_type}; non-coercible values become null."
                        ),
                    })

    return warnings
