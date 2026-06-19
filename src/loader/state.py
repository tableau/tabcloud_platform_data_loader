"""
Query database endpoint for current load state (e.g. last ingested timestamp, file list).
State contract is target-dependent; implement per target in targets/.
"""

from typing import Any, Optional


def get_state(target_config: dict) -> Optional[dict]:
    """
    Query the target for current ingestion state.

    :param target_config: Target-specific config (e.g. type, hyper_path for Hyper; name for others).
    :return: State dict (e.g. {"exists": bool, "tables": [...]} for Hyper) or None.
    """
    target_type = (target_config.get("type") or "").strip().lower()
    if target_type == "hyper":
        from loader.targets.hyper import get_hyper_state
        return get_hyper_state(target_config.get("hyper_path") or "")
    return None
