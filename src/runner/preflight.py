"""
Pre-flight authentication checks.

Verifies every TCM connection and every named Tableau destination BEFORE
any extraction runs.  Any failure aborts the run (non-zero exit).

This is intentionally lightweight: we sign in and immediately sign out.
We never fetch data in pre-flight; we only confirm credentials are valid.
"""

from __future__ import annotations

import logging
from typing import List, Tuple

from runner.config import RunConfig

logger = logging.getLogger("runner.preflight")


class PreflightError(RuntimeError):
    """Raised when one or more pre-flight checks fail."""


def run_preflight(cfg: RunConfig) -> None:
    """
    Authenticate against all connections and destinations.

    :raises PreflightError: if any authentication fails, listing all failures.
    """
    failures: List[str] = []

    # -- TCM connection --
    logger.info("Pre-flight: verifying TCM connection (%s) …", cfg.connection.tcm_url)
    ok, detail = _verify_tcm(cfg)
    if ok:
        logger.info("Pre-flight: TCM connection OK.")
    else:
        msg = f"TCM connection failed: {detail}"
        logger.error("Pre-flight: %s", msg)
        failures.append(msg)

    # -- Tableau destinations --
    for name, dest in cfg.destinations.items():
        logger.info(
            "Pre-flight: verifying destination '%s' (%s) …", name, dest.server_url
        )
        ok, detail = _verify_tableau(dest)
        if ok:
            logger.info("Pre-flight: destination '%s' OK.", name)
        else:
            msg = f"Destination '{name}' ({dest.server_url}) failed: {detail}"
            logger.error("Pre-flight: %s", msg)
            failures.append(msg)

    if failures:
        raise PreflightError(
            "Pre-flight authentication failed for the following targets:\n"
            + "\n".join(f"  • {f}" for f in failures)
        )


# ---------------------------------------------------------------------------
# Verifiers
# ---------------------------------------------------------------------------


def _verify_tcm(cfg: RunConfig) -> Tuple[bool, str]:
    """Sign in to the TCM API and immediately sign out."""
    try:
        from common.secrets import resolve_secret
        secret = resolve_secret(cfg.connection.token_secret)
    except Exception as exc:
        return False, f"Could not resolve token_secret: {exc}"

    try:
        import requests
        from extractor.retriever import LogRetriever
        from extractor.http_retry import tcm_api_error_from_exception
        from storage.local import LocalStorage
        import tempfile, os
        # Build a minimal retriever just for authentication
        with tempfile.TemporaryDirectory() as tmp:
            storage = LocalStorage(tmp)
            retriever = LogRetriever(
                pat_name=cfg.connection.token_name,
                pat_secret=secret,
                api_url=cfg.connection.tcm_url,
                site_ids=[],
                start_time="",
                end_time="",
                storage_backend=storage,
            )
            from extractor.retriever import create_operation_id
            success = retriever._login(create_operation_id())
            if not success:
                err = retriever._last_tcm_api_error
                detail = str(err) if err else "Sign-in returned False"
                return False, detail
            # Immediately discard the session; no sign-out endpoint in TCM.
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _verify_tableau(dest) -> Tuple[bool, str]:
    """Sign in to Tableau Server/Cloud and immediately sign out."""
    try:
        from common.secrets import resolve_secret
        secret = resolve_secret(dest.token_secret)
    except Exception as exc:
        return False, f"Could not resolve token_secret: {exc}"

    try:
        from loader.targets.tableau_publish import sign_in, sign_out
        token, site_id, api_url = sign_in(
            server_url=dest.server_url,
            token_name=dest.token_name,
            token_secret=secret,
            site_content_url=dest.site,
        )
        sign_out(api_url, token)
        return True, ""
    except Exception as exc:
        return False, str(exc)
