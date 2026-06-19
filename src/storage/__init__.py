"""
Multi-backend storage abstraction for Extractor, Transformer, and Loader.

This package re-exports every public symbol so that existing code using
``from storage import LocalStorage`` (or any other name) continues to work.
"""

from storage.base import (
    StorageBackend,
    DEFAULT_STORAGE_BASE,
    EXTRACT_FOLDER,
    TRANSFORM_FOLDER,
    LOADER_FOLDER,
    LOGS_FOLDER,
    get_extract_path,
    get_transform_path,
    get_parquet_path,
    get_loader_path,
    get_logs_path,
    extract_event_type_from_path,
    extract_segment_value_from_path,
    is_valid_parquet_bytes,
    is_valid_parquet_file,
)
from storage.local import LocalStorage
from storage.config import load_storage_config, create_storage_backend, create_component_backend
from storage.presence import build_presence_set

try:
    from storage.s3 import S3Storage
except ImportError:
    S3Storage = None  # s3fs not installed

__all__ = [
    "StorageBackend",
    "LocalStorage",
    "S3Storage",
    "DEFAULT_STORAGE_BASE",
    "EXTRACT_FOLDER",
    "TRANSFORM_FOLDER",
    "LOADER_FOLDER",
    "LOGS_FOLDER",
    "get_extract_path",
    "get_transform_path",
    "get_parquet_path",
    "get_loader_path",
    "get_logs_path",
    "extract_event_type_from_path",
    "extract_segment_value_from_path",
    "is_valid_parquet_bytes",
    "is_valid_parquet_file",
    "load_storage_config",
    "create_storage_backend",
    "create_component_backend",
    "build_presence_set",
]
