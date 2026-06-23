"""Local-disk storage backend."""

import os

from storage.base import StorageBackend


def _safe_join(base: str, relative_path: str) -> str:
    """Join *base* and *relative_path*, raising :class:`ValueError` if the
    result escapes *base*.

    Guards against:
    - Absolute ``relative_path`` values (``os.path.join`` would silently
      discard *base* and return the absolute path as-is).
    - ``..`` traversal that climbs out of *base* after symlink resolution.

    *base* must already be an absolute path (as produced by ``os.path.abspath``
    in :meth:`LocalStorage.__init__`).
    """
    if os.path.isabs(relative_path):
        raise ValueError(
            f"relative_path must not be absolute, got: {relative_path!r}"
        )
    parts = relative_path.replace("\\", "/").split("/")
    if ".." in parts:
        raise ValueError(
            f"relative_path contains '..' traversal segment: {relative_path!r}"
        )
    target = os.path.realpath(os.path.join(base, relative_path))
    real_base = os.path.realpath(base)
    if not (target == real_base or target.startswith(real_base + os.sep)):
        raise ValueError(
            f"Path {relative_path!r} resolves outside storage root {base!r}"
        )
    return target


class LocalStorage(StorageBackend):
    """Storage backend that reads/writes on the local filesystem."""

    def __init__(self, base_path: str):
        self._base_path = os.path.abspath(base_path)
        os.makedirs(self._base_path, exist_ok=True)

    # -- properties ---------------------------------------------------------

    @property
    def base_path(self) -> str:
        return self._base_path

    # -- listing / querying -------------------------------------------------

    def list_files(self, prefix="") -> list:
        files = []
        if prefix:
            try:
                full_prefix_path = _safe_join(self._base_path, prefix)
            except ValueError:
                return files
        else:
            full_prefix_path = self._base_path
        if not os.path.exists(full_prefix_path):
            return files
        for dirpath, _, filenames in os.walk(full_prefix_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                relative_path = os.path.relpath(full_path, self._base_path)
                files.append(relative_path.replace("\\", "/"))
        return files

    def exists(self, relative_path: str) -> bool:
        try:
            target = _safe_join(self._base_path, relative_path)
        except ValueError:
            return False
        return os.path.exists(target)

    # -- reading ------------------------------------------------------------

    def read_file(self, relative_path: str) -> str:
        with open(_safe_join(self._base_path, relative_path), "r", encoding="utf-8") as f:
            return f.read()

    def read_bytes(self, relative_path: str) -> bytes:
        with open(_safe_join(self._base_path, relative_path), "rb") as f:
            return f.read()

    def read_first_line(self, relative_path: str):
        with open(_safe_join(self._base_path, relative_path), "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    return line
        return None

    # -- writing ------------------------------------------------------------

    def write_bytes(self, relative_path: str, data: bytes) -> None:
        target = _safe_join(self._base_path, relative_path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        # Write to a temp sibling then atomically rename so a partially-written
        # file is never visible at the final path (e.g. if the process is killed
        # mid-write or a validation exception is raised by the caller).
        tmp = target + ".part"
        try:
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, target)
        except Exception:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise

    def write_file(self, relative_path: str, content: str) -> None:
        self.write_bytes(relative_path, content.encode("utf-8"))

    # -- paths / URIs -------------------------------------------------------

    def full_path(self, relative_path: str) -> str:
        return _safe_join(self._base_path, relative_path)

    def uri(self, relative_path: str) -> str:
        return _safe_join(self._base_path, relative_path)
