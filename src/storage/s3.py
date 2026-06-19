"""S3 storage backend (requires *s3fs*)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from storage.base import StorageBackend


def default_s3fs_config_kwargs() -> Dict[str, Any]:
    """Default ``botocore`` ``Config`` kwargs: retries and connect/read timeouts."""
    return {
        "retries": {"max_attempts": 10, "mode": "adaptive"},
        "connect_timeout": 60,
        "read_timeout": 60,
    }


def merge_s3fs_config_kwargs(
    base: Dict[str, Any], override: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Shallow+merge nested ``retries`` for botocore ``Config``."""
    if not override:
        return dict(base)
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict) and k == "retries":
            out[k] = {**out[k], **v}
        else:
            out[k] = v
    return out


class S3Storage(StorageBackend):
    """Storage backend that reads/writes objects in an S3-compatible bucket.

    Uses explicit ``s3fs``/botocore retry and timeout defaults (``config_kwargs``), with
    optional ``max_concurrency`` and ``default_block_size`` for multipart-style uploads.
    For assumed-role or instance credentials, use ``aws_default_chain`` or ``aws_profile``;
    static key/secret from config (e.g. Secrets Manager) are not long-lived-refreshing.

    Dependencies (``s3fs``, ``fsspec``) are imported lazily so the rest of
    the codebase can run without them when only local storage is used.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: str | None = None,
        endpoint_url: str | None = None,
        **s3_options,
    ):
        import s3fs  # lazy import

        self._bucket = bucket
        self._prefix = prefix.strip("/")

        client_kwargs = {}
        if region:
            client_kwargs["region_name"] = region
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        # Botocore Config: merge defaults with optional ``config_kwargs`` from storage YAML
        user_cfg = s3_options.pop("config_kwargs", None)
        if user_cfg is not None and not isinstance(user_cfg, dict):
            raise TypeError("config_kwargs must be a dict when provided")
        config_kwargs = merge_s3fs_config_kwargs(default_s3fs_config_kwargs(), user_cfg)

        # s3fs transfer / multipart–friendly top-level options (optional in YAML)
        max_concurrency = s3_options.pop("max_concurrency", 10)
        default_block_size = s3_options.pop("default_block_size", None)
        fixed_upload_size = s3_options.pop("fixed_upload_size", None)

        self._fs = s3fs.S3FileSystem(
            client_kwargs=client_kwargs or None,
            config_kwargs=config_kwargs,
            max_concurrency=max_concurrency,
            default_block_size=default_block_size,
            fixed_upload_size=fixed_upload_size,
            **s3_options,
        )

    # -- helpers ------------------------------------------------------------

    def _key(self, relative_path: str) -> str:
        """Full S3 key for *relative_path*."""
        if self._prefix:
            return f"{self._prefix}/{relative_path}"
        return relative_path

    def _s3_uri(self, key: str) -> str:
        return f"s3://{self._bucket}/{key}"

    # -- properties ---------------------------------------------------------

    @property
    def base_path(self) -> str:
        if self._prefix:
            return f"s3://{self._bucket}/{self._prefix}"
        return f"s3://{self._bucket}"

    # -- listing / querying -------------------------------------------------

    def list_files(self, prefix="") -> list:
        search_prefix = self._key(prefix) if prefix else (self._prefix or "")
        full_prefix = f"{self._bucket}/{search_prefix}" if search_prefix else self._bucket

        try:
            all_keys = self._fs.find(full_prefix, detail=False)
        except FileNotFoundError:
            return []

        base = f"{self._bucket}/{self._prefix}" if self._prefix else self._bucket
        results = []
        for key in all_keys:
            if key.startswith(base + "/"):
                rel = key[len(base) + 1:]
            elif key == base:
                continue
            else:
                rel = key.split("/", 1)[-1] if "/" in key else key
            results.append(rel)
        return results

    def exists(self, relative_path: str) -> bool:
        return self._fs.exists(self._s3_uri(self._key(relative_path)))

    # -- reading ------------------------------------------------------------

    def read_file(self, relative_path: str) -> str:
        return self.read_bytes(relative_path).decode("utf-8")

    def read_bytes(self, relative_path: str) -> bytes:
        with self._fs.open(self._s3_uri(self._key(relative_path)), "rb") as f:
            return f.read()

    def read_first_line(self, relative_path: str):
        with self._fs.open(self._s3_uri(self._key(relative_path)), "rb") as f:
            for raw_line in f:
                line = raw_line.decode("utf-8").strip()
                if line:
                    return line
        return None

    # -- writing ------------------------------------------------------------

    def write_bytes(self, relative_path: str, data: bytes) -> None:
        with self._fs.open(self._s3_uri(self._key(relative_path)), "wb") as f:
            f.write(data)

    def write_file(self, relative_path: str, content: str) -> None:
        self.write_bytes(relative_path, content.encode("utf-8"))

    # -- paths / URIs -------------------------------------------------------

    def full_path(self, relative_path: str) -> str:
        return self._s3_uri(self._key(relative_path))

    def uri(self, relative_path: str) -> str:
        return self.full_path(relative_path)
