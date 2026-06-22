"""
Interactive bootstrap for when run.yml is absent or has missing required values.

Behavior:
  • TTY context: warn, offer bare-minimum prompt, optionally store secret in
    keyring and save a run.yml for reuse.
  • Non-TTY context (cron, pipe, CI): print guidance and exit non-zero.

Only TCM credentials are prompted (the bare minimum to run the default pipeline).
Destinations and advanced settings require editing run.yml directly.
"""

from __future__ import annotations

import getpass
import os
import re
import sys
import textwrap
from typing import Optional

# ---------------------------------------------------------------------------
# Template location
# ---------------------------------------------------------------------------

# src/runner/bootstrap.py → project root is 3 levels up
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_TEMPLATE_PATH = os.path.join(_PROJECT_ROOT, "config", "run.example.yml")

# ---------------------------------------------------------------------------
# Secret-ref helpers
# ---------------------------------------------------------------------------

_SECRET_REF_PREFIXES = ("keyring:", "env:", "file:", "awssm:", "plain:")


def _is_secret_ref(s: str) -> bool:
    """Return True when *s* is a recognised secret-reference scheme, not a bare literal."""
    return any(s.startswith(p) for p in _SECRET_REF_PREFIXES)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def maybe_bootstrap(config_path: str) -> Optional[str]:
    """
    If ``config_path`` does not exist (or has missing required fields), guide the
    user through a minimal interactive setup.

    :param config_path: Desired path for run.yml.
    :returns: The path that was written (possibly the same as config_path), or
              None if the user declined and the caller should re-try loading.
    :raises SystemExit: In non-TTY contexts, or when the user declines on TTY.
    """
    config_path = os.path.abspath(config_path)
    missing = not os.path.isfile(config_path)

    if not sys.stdin.isatty():
        _non_tty_exit(config_path, missing)

    return _interactive_bootstrap(config_path, missing)


# ---------------------------------------------------------------------------
# Non-TTY path
# ---------------------------------------------------------------------------


def _non_tty_exit(config_path: str, missing: bool) -> None:
    if missing:
        msg = textwrap.dedent(f"""\
            ERROR: run.yml not found at: {config_path}

            Create a run.yml to use tabcloud-run. Minimum content:

              connection:
                tcm_url:      https://your-tenant.online.tableau.com
                token_name:   your-pat-name
                token_secret: keyring:tabcloud/extractor-pat   # or env:MY_PAT_SECRET

            Store the secret once (recommended):
              tabcloud-secrets set tabcloud extractor-pat

            Then run:
              tabcloud-run -c {config_path}

            See config/run.example.yml for a fully-annotated reference.
        """)
    else:
        msg = textwrap.dedent(f"""\
            ERROR: run.yml at {config_path} is missing required values
            (connection.tcm_url, connection.token_name, or connection.token_secret).

            Edit the file and fill in the missing values, then retry.
        """)
    print(msg, file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Interactive (TTY) path
# ---------------------------------------------------------------------------


def _interactive_bootstrap(config_path: str, missing: bool) -> Optional[str]:
    print()
    if missing:
        print(f"  No run.yml found at: {config_path}")
    else:
        print(f"  run.yml at {config_path} is missing required values.")
    print()
    print("  tabcloud-run needs a few details to connect to Tableau Cloud Manager.")
    print("  You can answer the prompts below for a bare-minimum run, or press")
    print("  Ctrl-C to exit and edit run.yml manually.")
    print()

    try:
        answer = input("  Proceed with interactive setup? [y/N] ").strip().lower()
    except (KeyboardInterrupt, EOFError):
        print()
        sys.exit(0)

    if answer not in ("y", "yes"):
        print()
        print(f"  Edit {config_path} and re-run.")
        print(f"  See config/run.example.yml for a fully-annotated reference.")
        sys.exit(0)

    print()
    print("  Enter your Tableau Cloud Manager (TCM) connection details.")
    print("  These are your TCM Personal Access Token credentials.")
    print()

    tcm_url = _prompt("  TCM URL (e.g. https://your-tenant.online.tableau.com): ").rstrip("/")
    token_name = _prompt("  PAT name: ")
    secret_plain = getpass.getpass("  PAT secret (input hidden): ")

    # Offer keyring storage
    secret_ref = _offer_keyring(secret_plain)

    # Ask where to save run.yml (default ./run.yml; "no"/"n" → temp file)
    default_save_path = os.path.abspath("./run.yml")
    config_path = _prompt_save_path(default_save_path)

    # Ask where pipeline artifacts should live. Default to a location next to
    # the config (but never *inside* a `config/` dir) so data does not get
    # buried alongside run.yml.
    storage_base = os.getcwd() if config_path is None else os.path.dirname(config_path)
    storage_path = _prompt_storage_path(storage_base)

    if config_path is None:
        # User declined to save — write a temp file so the caller can load it.
        # No plaintext warning for ephemeral temp files.
        import tempfile
        fd, tmp_path = tempfile.mkstemp(suffix=".yml", prefix="tabcloud-run-")
        os.close(fd)
        _write_full_run_yml(tmp_path, tcm_url, token_name, secret_ref, storage_path)
        config_path = tmp_path
        print(f"\n  Using temporary config: {config_path}")
        print(f"  Storage: {storage_path}")
        print("  (Not saved — pass -c <path> next time to use a persistent file)")
    else:
        # Persistent file — warn loudly if the secret would be stored as plaintext.
        if not _is_secret_ref(secret_ref):
            secret_ref = _warn_plaintext_and_offer_env(secret_plain)
        _write_full_run_yml(config_path, tcm_url, token_name, secret_ref, storage_path)
        print(f"\n  Saved: {config_path}")
        print(f"  Storage: {storage_path}")
        print(f"  Run again with:  tabcloud-run -c {config_path}")

    print()
    return config_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _prompt(msg: str) -> str:
    while True:
        try:
            value = input(msg).strip()
        except (KeyboardInterrupt, EOFError):
            print()
            sys.exit(0)
        if value:
            return value
        print("  (required — cannot be empty)")


def _prompt_save_path(default_path: str) -> Optional[str]:
    """
    Ask the user where to save run.yml.

    :returns: Absolute path to save the file, or None when the user types
              'no'/'n' (use a temp file for this run only).
    """
    print()
    print(f"  Where should the configuration be saved?")
    print(f"  Press Enter to use the default, type a path, or type 'no' to skip saving.")
    try:
        answer = input(f"  Save path [default: {default_path}]: ").strip()
    except (KeyboardInterrupt, EOFError):
        print()
        sys.exit(0)

    if answer.lower() in ("no", "n"):
        return None
    if not answer:
        return default_path
    # Expand ~ and make absolute
    return os.path.abspath(os.path.expanduser(answer))


def _default_storage_path(config_dir: str) -> str:
    """
    Choose a sensible default storage location given the directory that will
    hold run.yml.

    Data should not be buried inside a ``config/`` directory, so when the
    config lives in a folder literally named ``config`` the default storage
    is placed as a sibling of that folder instead of underneath it.
    """
    config_dir = os.path.abspath(config_dir)
    if os.path.basename(config_dir).lower() == "config":
        base = os.path.dirname(config_dir)
    else:
        base = config_dir
    return os.path.join(base, "data_workspace")


def _prompt_storage_path(config_dir: str) -> str:
    """
    Ask where pipeline artifacts (extracts, transforms, logs) should be stored.

    :param config_dir: Directory that will hold run.yml; used to anchor the
                        default storage location.
    :returns: Absolute path to the storage root.
    """
    default_path = _default_storage_path(config_dir)
    print()
    print("  Where should pipeline data be stored?")
    print("  This holds extracts, transformed output, and logs (can be large).")
    print("  Press Enter to use the default, or type a path.")
    try:
        answer = input(f"  Storage path [default: {default_path}]: ").strip()
    except (KeyboardInterrupt, EOFError):
        print()
        sys.exit(0)

    if not answer:
        return default_path
    # Expand ~ and make absolute
    return os.path.abspath(os.path.expanduser(answer))


def _offer_keyring(secret_plain: str) -> str:
    """
    Offer to store the secret in keyring. Returns the appropriate secret_ref
    string (either keyring:... or a bare literal for in-memory only use).
    """
    try:
        import keyring as _kr  # noqa: F401
        keyring_available = True
    except ImportError:
        keyring_available = False

    if not keyring_available:
        print()
        print("  (keyring not installed; secret will be used for this run only)")
        print("  Install with: pip install 'tabcloud_platform_data_loader[secrets]'")
        return secret_plain

    print()
    try:
        store = input(
            "  Store the PAT secret in your OS keychain? [Y/n] "
        ).strip().lower()
    except (KeyboardInterrupt, EOFError):
        return secret_plain

    if store not in ("", "y", "yes"):
        return secret_plain

    service = "tabcloud"
    username = "extractor-pat"
    try:
        import keyring
        keyring.set_password(service, username, secret_plain)
        ref = f"keyring:{service}/{username}"
        print(f"  Stored. Reference: {ref}")
        return ref
    except Exception as exc:
        print(f"  Could not store in keyring ({exc}). Secret kept in-memory only.")
        return secret_plain


def _warn_plaintext_and_offer_env(secret_plain: str) -> str:
    """
    Warn that the secret will be written as plaintext to the saved file.

    Prompts the user to either switch to an env: reference or confirm
    writing the plaintext anyway.

    :param secret_plain: The raw secret value entered by the user.
    :returns: Final secret_ref string — either ``env:VAR`` or the bare literal.
    """
    print()
    print("  WARNING: the PAT secret will be stored as PLAINTEXT in the saved file.")
    print("     Anyone who can read that file will see the secret.")
    print("     Do not commit this file to version control.")
    print()
    print("  To keep secrets out of the file, use a secret reference instead:")
    print("    keyring:SERVICE/USERNAME  — OS keychain (recommended)")
    print("    env:VAR_NAME              — environment variable")
    print("    file:/path/to/secret      — file contents (newline stripped)")
    print()
    print("  Options:")
    print("    1. Use an env: reference (recommended)")
    print("    2. Write plaintext anyway (not recommended)")
    print()

    while True:
        try:
            choice = input("  Choose [1/2]: ").strip()
        except (KeyboardInterrupt, EOFError):
            print()
            sys.exit(0)

        if choice == "1":
            try:
                var_name = input(
                    "  Environment variable name [default: TABCLOUD_PAT_SECRET]: "
                ).strip()
            except (KeyboardInterrupt, EOFError):
                print()
                sys.exit(0)
            if not var_name:
                var_name = "TABCLOUD_PAT_SECRET"
            ref = f"env:{var_name}"
            print()
            print(f"  Set this variable before running tabcloud-run:")
            print(f"    export {var_name}='<your-secret-here>'")
            print(f"  Reference stored in file: {ref}")
            return ref

        elif choice == "2":
            return secret_plain

        else:
            print("  (please enter 1 or 2)")


# ---------------------------------------------------------------------------
# Template loading and config writing
# ---------------------------------------------------------------------------


def _load_template() -> str:
    """
    Return the contents of run.example.yml.

    Falls back to a minimal YAML stub when the template file cannot be
    located (e.g. in a stripped package installation without the config/ dir).
    """
    if os.path.isfile(_TEMPLATE_PATH):
        with open(_TEMPLATE_PATH, encoding="utf-8") as f:
            return f.read()

    # Minimal fallback — enough for _write_full_run_yml substitutions to work.
    return textwrap.dedent("""\
        # run.yml — tabcloud-run configuration
        # Generated by interactive bootstrap.
        # See config/run.example.yml for a fully-annotated reference.

        version: 1

        connection:
          tcm_url:      https://your-tenant.online.tableau.com   # required
          token_name:   your-pat-name                            # required
          token_secret: keyring:tabcloud/extractor-pat           # required; use a secret_ref

        # All other blocks (extraction, transform, load, storage, logging) use
        # defaults. Edit this file to customise the pipeline.
    """)


def _write_full_run_yml(
    path: str,
    tcm_url: str,
    token_name: str,
    secret_ref: str,
    storage_path: str = "",
) -> None:
    """
    Write a run.yml by substituting the connection values (and storage path)
    into the run.example.yml template, preserving all comments and other
    sections.

    Relative ``mapping:`` paths (e.g. ``mappings/transformer/foo.yaml``) are
    rewritten to absolute paths anchored at the project root so the file works
    correctly no matter where it is saved.

    The ``storage.path`` value is replaced with *storage_path* (an absolute
    directory) when provided, so pipeline artifacts land where the user asked
    rather than relative to the saved config.

    If *secret_ref* is a bare literal (not a recognised secret-reference
    scheme) an inline warning comment is appended to the token_secret line.
    """
    from runner.config import bundled_mappings_root

    template = _load_template()

    # Replace only the value portion of each connection line; preserve
    # any trailing inline comment.  Lambda replacements are safe against
    # backslashes or other regex metacharacters in user-supplied values.
    content = re.sub(
        r'^(\s+tcm_url:\s+)\S+',
        lambda m: m.group(1) + tcm_url,
        template, flags=re.MULTILINE,
    )
    content = re.sub(
        r'^(\s+token_name:\s+)\S+',
        lambda m: m.group(1) + token_name,
        content, flags=re.MULTILINE,
    )
    content = re.sub(
        r'^(\s+token_secret:\s+)\S+',
        lambda m: m.group(1) + secret_ref,
        content, flags=re.MULTILINE,
    )

    # Rewrite relative bundled mapping paths to absolute so the file works
    # regardless of where it is saved.  Only un-commented lines are affected
    # (commented lines start with # so ^\s+mapping: won't match them).
    root = bundled_mappings_root()
    content = re.sub(
        r'^(\s+mapping:\s+)(mappings/)',
        lambda m: m.group(1) + root + "/" + m.group(2),
        content, flags=re.MULTILINE,
    )

    # Substitute the storage path so artifacts land where the user chose.
    # The template's only `path:` key lives under storage:.
    if storage_path:
        content = re.sub(
            r'^(\s+path:\s+)\S+.*$',
            lambda m: m.group(1) + storage_path + "  # local artifact root",
            content, flags=re.MULTILINE,
        )

    # When the secret is a bare literal, mark the line so it is easy to spot.
    if not _is_secret_ref(secret_ref):
        content = re.sub(
            r'^(\s+token_secret:\s+\S+)',
            r'\1  # WARNING: plaintext — consider keyring: or env: instead',
            content, flags=re.MULTILINE,
        )

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
