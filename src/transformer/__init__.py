"""
Transformer: reads JSON from extract storage (extract/), applies mapping configs, writes Parquet or CSV to output dir.
"""

from transformer.reader import discover_json_files, discover_files_by_source, load_json_records
from transformer.mapping import (
    load_mapping_config,
    apply_input_mapping,
    get_schema_field_names,
    get_schema_types,
)
from transformer.pipeline import run_pipeline
from transformer.parquet_io import write_table_parquet
from transformer.csv_io import write_table_csv

__all__ = [
    "discover_json_files",
    "discover_files_by_source",
    "load_json_records",
    "load_mapping_config",
    "apply_input_mapping",
    "get_schema_field_names",
    "get_schema_types",
    "run_pipeline",
    "write_table_parquet",
    "write_table_csv",
]
