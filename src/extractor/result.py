"""Structured result from a LogRetriever run (CLI exit codes, orchestration)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Literal


@dataclass
class ExtractionResult:
    status: Literal["success", "degraded", "failed"]
    """success: all required downloads present; degraded: some paths missing; failed: auth/validation."""
    failed_paths: List[str] = field(default_factory=list)
    expected_downloads: int = 0
    """Total API file paths that were required to be present in storage (after pre-filter)."""
    note: str = ""
