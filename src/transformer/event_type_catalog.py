"""
Helpers for cataloging event types in extract storage and mapping coverage.
"""

from __future__ import annotations

import os
from typing import Dict, Iterable, List, Set

from storage import extract_event_type_from_path
from transformer.mapping import load_mapping_config


def discover_event_types_in_extract(storage_backend) -> List[str]:
    """Return sorted unique event types discovered in extract files."""
    event_types: Set[str] = set()
    for rel_path in storage_backend.list_files(prefix=""):
        event_type = extract_event_type_from_path(rel_path)
        if event_type:
            event_types.add(event_type)
    return sorted(event_types)


def _iter_transformer_mapping_files(mappings_dir: str) -> Iterable[str]:
    if not os.path.isdir(mappings_dir):
        return []
    files = []
    for name in sorted(os.listdir(mappings_dir)):
        if name.lower().endswith((".yaml", ".yml")):
            files.append(os.path.join(mappings_dir, name))
    return files


def load_mapping_event_type_coverage(mappings_dir: str) -> Dict[str, List[str]]:
    """
    Return event type -> mapping-file-list coverage from transformer mappings directory.
    """
    coverage: Dict[str, List[str]] = {}
    for mapping_path in _iter_transformer_mapping_files(mappings_dir):
        config = load_mapping_config(mapping_path) or {}
        for input_cfg in config.get("inputs") or []:
            event_type = (input_cfg.get("eventType") or "").strip()
            if not event_type:
                continue
            coverage.setdefault(event_type, []).append(mapping_path)
    return coverage


def build_coverage_report(
    discovered_event_types: List[str], mapping_coverage: Dict[str, List[str]]
) -> dict:
    """Create a summary dict for discovered types and mapping coverage."""
    covered = [et for et in discovered_event_types if et in mapping_coverage]
    uncovered = [et for et in discovered_event_types if et not in mapping_coverage]
    return {
        "discovered_event_types": discovered_event_types,
        "covered_event_types": covered,
        "uncovered_event_types": uncovered,
        "mapping_coverage": mapping_coverage,
    }


def write_generic_auto_mapping(
    output_path: str,
    discovered_event_types: List[str],
    output_filename: str = "generic_activity_log_auto",
) -> str:
    """
    Write an auto-schema transformer mapping with one input per discovered event type.
    """
    try:
        import yaml
    except ImportError as exc:
        raise ImportError("PyYAML is required to generate mapping files.") from exc

    inputs = [
        {
            "eventType": event_type,
            "source": {
                "folder_pattern": f"**/eventType={event_type}",
                "file_pattern": "*.json",
                "recursive": True,
            },
        }
        for event_type in discovered_event_types
    ]
    if not inputs:
        inputs.append(
            {
                "eventType": "",
                "source": {
                    "folder_pattern": "**/eventType=*",
                    "file_pattern": "*.json",
                    "recursive": True,
                },
            }
        )

    mapping_doc = {
        "output_filename": output_filename,
        "output": {
            "format": "parquet",
            "target_path": "",
            "partition_by": [],
            "schema": "auto",
        },
        "inputs": inputs,
    }
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(mapping_doc, fh, sort_keys=False)
    return output_path
