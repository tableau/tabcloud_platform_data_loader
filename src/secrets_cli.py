"""
tabcloud-secrets — manage secrets for the TabCloud Platform Data Loader.

What is keyring?
----------------
keyring is a Python library that lets applications store and retrieve passwords
using the OS's built-in secure storage — the same place your browser and other
applications store your passwords.  On macOS this is the Keychain, on Windows
it is the Credential Locker, and on Linux it is the Secret Service (GNOME
Keyring, KWallet, or similar).  The secret never touches a config file or
command-line argument.

Quick start
-----------
1. Install keyring support (once per environment):

     pip install -e .[secrets]

2. Store a secret (you will be prompted; nothing is echoed to the terminal):

     tabcloud-secrets set tabcloud extractor-pat

3. Reference it in your config (no secret value in the file):

     token_secret = keyring:tabcloud/extractor-pat

4. Verify everything is working:

     tabcloud-secrets doctor

Subcommands
-----------
  set SERVICE USERNAME    Store a secret in the OS keychain.
  get SERVICE USERNAME    Retrieve and print a stored secret.
  delete SERVICE USERNAME Remove a stored secret.
  list SERVICE            List usernames stored under SERVICE (not all backends).
  doctor                  Show which keyring backend is active and whether it is
                          a real secure store, with OS-specific guidance.
"""

from __future__ import annotations

import argparse
import getpass
import sys
import textwrap

# ---------------------------------------------------------------------------
# Shared install check
# ---------------------------------------------------------------------------

_INSTALL_HINT = (
    "Install keyring support:  pip install -e .[secrets]\n"
    "Then re-run this command."
)

_EPILOG = textwrap.dedent("""\
    Quick start:
      pip install -e .[secrets]
      tabcloud-secrets set tabcloud extractor-pat
      # Then in config.ini:
      #   token_secret = keyring:tabcloud/extractor-pat

    Run 'tabcloud-secrets doctor' to verify your keyring backend.
    Run 'tabcloud-secrets --help' for full usage.
""")


def _require_keyring():
    try:
        import keyring
        return keyring
    except ImportError:
        print("ERROR: keyring is not installed.", file=sys.stderr)
        print(_INSTALL_HINT, file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Subcommand: set
# ---------------------------------------------------------------------------

def cmd_set(args):
    """Store a secret in the OS keychain (prompts for the value)."""
    keyring = _require_keyring()
    service = args.service
    username = args.username

    print(f"Enter the secret for service={service!r} username={username!r}")
    print("(Input is hidden — nothing will be shown as you type.)")
    try:
        secret = getpass.getpass("Secret: ")
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        sys.exit(1)

    if not secret:
        print("ERROR: Secret cannot be empty.", file=sys.stderr)
        sys.exit(1)

    try:
        keyring.set_password(service, username, secret)
    except Exception as exc:
        print(f"ERROR: Could not store secret: {exc}", file=sys.stderr)
        print("Run 'tabcloud-secrets doctor' to diagnose keyring issues.", file=sys.stderr)
        sys.exit(1)

    ref = f"keyring:{service}/{username}"
    print(f"\nStored successfully.")
    print(f"\nReference it in your config as:")
    print(f"    token_secret = {ref}")
    print(f"\nOr for publish PAT:")
    print(f"    token_secret = {ref}")


# ---------------------------------------------------------------------------
# Subcommand: get
# ---------------------------------------------------------------------------

def cmd_get(args):
    """Retrieve and print a stored secret (use with care — output is visible)."""
    keyring = _require_keyring()
    service = args.service
    username = args.username

    try:
        value = keyring.get_password(service, username)
    except Exception as exc:
        print(f"ERROR: Keyring lookup failed: {exc}", file=sys.stderr)
        print("Run 'tabcloud-secrets doctor' to diagnose.", file=sys.stderr)
        sys.exit(1)

    if value is None:
        print(
            f"No secret found for service={service!r} username={username!r}.",
            file=sys.stderr,
        )
        print(
            f"Store it first: tabcloud-secrets set {service} {username}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(value)


# ---------------------------------------------------------------------------
# Subcommand: delete
# ---------------------------------------------------------------------------

def cmd_delete(args):
    """Remove a stored secret from the OS keychain."""
    keyring = _require_keyring()
    service = args.service
    username = args.username

    try:
        keyring.delete_password(service, username)
        print(f"Deleted secret for service={service!r} username={username!r}.")
    except keyring.errors.PasswordDeleteError:
        print(
            f"No secret found for service={service!r} username={username!r}.",
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: Could not delete secret: {exc}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Subcommand: list
# ---------------------------------------------------------------------------

def cmd_list(args):
    """List usernames stored under SERVICE (not all backends support this)."""
    keyring = _require_keyring()
    service = args.service

    # keyring.get_credential is the closest standard API; listing all usernames
    # is backend-dependent.  We attempt it via the backend's internal API
    # when available, and degrade gracefully.
    backend = keyring.get_keyring()
    backend_name = type(backend).__name__

    # Try keyring_getpass or SecretService via the generic backend API.
    if hasattr(backend, "get_all_credentials"):
        try:
            creds = backend.get_all_credentials(service)
            if creds:
                for c in creds:
                    print(c.username)
            else:
                print(f"No credentials found for service={service!r}.")
        except Exception:
            print(
                f"Backend {backend_name!r} does not support listing. "
                f"Use 'tabcloud-secrets get {service} <username>' to test individual entries.",
                file=sys.stderr,
            )
    else:
        print(
            f"Backend {backend_name!r} does not support listing all credentials for a service.",
            file=sys.stderr,
        )
        print(
            f"Use 'tabcloud-secrets get {service} <username>' to verify a specific entry.",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Subcommand: doctor
# ---------------------------------------------------------------------------

_OS_NOTES = {
    "macOS": textwrap.dedent("""\
        macOS Keychain is the active backend — this is a real, encrypted secure store.
        If you get a 'user interaction not allowed' error, unlock your Keychain first
        (Keychain Access.app or: security unlock-keychain ~/Library/Keychains/login.keychain-db).
    """),
    "Windows": textwrap.dedent("""\
        Windows Credential Locker is the active backend — this is a real, encrypted secure store.
        Secrets are tied to your Windows user account.
    """),
    "Linux": textwrap.dedent("""\
        Linux: the recommended backend is the Secret Service (GNOME Keyring or KWallet).
        If neither is available, keyring falls back to an unencrypted file store
        (keyrings.alt.file.PlaintextKeyring) which is NOT secure.

        To install a proper Secret Service:
          Ubuntu/Debian:  sudo apt-get install gnome-keyring libsecret-1-0
          Arch:           sudo pacman -S gnome-keyring
          Fedora:         sudo dnf install gnome-keyring

        Headless / CI servers: use env: or file: references instead of keyring:
          - env:VAR_NAME       — set the env var in your CI secrets/env config.
          - file:/path/secret  — write the secret to a 600-permission file.
          - awssm:SECRET_ID    — retrieve from AWS Secrets Manager (needs boto3).
    """),
}


def cmd_doctor(args):
    """Report the active keyring backend and whether it is a real secure store."""
    keyring = _require_keyring()
    import platform

    backend = keyring.get_keyring()
    backend_name = type(backend).__module__ + "." + type(backend).__name__
    backend_short = type(backend).__name__

    print(f"Keyring library version : {keyring.__version__ if hasattr(keyring, '__version__') else 'unknown'}")
    print(f"Active backend          : {backend_name}")

    # Classify the backend.
    plaintext_backends = {"PlaintextKeyring", "UncryptedKeyring", "BasicFileKeyring"}
    null_backends = {"NullKeyring", "FailKeyring", "Unavailable"}
    is_secure = not any(name in backend_short for name in plaintext_backends | null_backends)
    is_null = any(name in backend_short for name in null_backends)

    if is_null:
        print("\nWARNING: No functional keyring backend found.")
        print("Secrets stored with keyring: will NOT be retrievable.")
    elif not is_secure:
        print("\nWARNING: Backend is a plaintext file store — NOT encrypted.")
        print("Secrets are stored in cleartext on disk. Not recommended for production.")
    else:
        print("\nBackend is a real secure (encrypted) store.")

    # OS-specific guidance.
    os_name = platform.system()
    if os_name in _OS_NOTES:
        print()
        print(_OS_NOTES[os_name].strip())

    # Round-trip test.
    print()
    print("Running a round-trip test (set / get / delete a temporary secret)...")
    test_svc = "tabcloud-doctor-test"
    test_user = "test"
    test_val = "tabcloud-doctor-ok"
    try:
        keyring.set_password(test_svc, test_user, test_val)
        retrieved = keyring.get_password(test_svc, test_user)
        keyring.delete_password(test_svc, test_user)
        if retrieved == test_val:
            print("Round-trip test: PASSED. Keyring is working correctly.")
        else:
            print("Round-trip test: FAILED — value did not match.", file=sys.stderr)
    except Exception as exc:
        print(f"Round-trip test: FAILED — {exc}", file=sys.stderr)
        if os_name == "Linux" and not is_null:
            print(
                "\nHint: on Linux/headless, start the Secret Service daemon first:\n"
                "  eval $(gnome-keyring-daemon --start 2>/dev/null)",
                file=sys.stderr,
            )

    print()
    print("Alternatives to keyring (useful on headless servers or CI):")
    print("  env:VAR_NAME       — export MY_PAT=<secret>; use token_secret = env:MY_PAT")
    print("  file:/path/secret  — write secret to file (chmod 600); use token_secret = file:/path/secret")
    print("  awssm:SECRET_ID    — AWS Secrets Manager; use token_secret = awssm:my/secret#field")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="tabcloud-secrets",
        description=(
            "Manage secrets for the TabCloud Platform Data Loader.\n\n"
            "Secrets are stored in the OS keychain (macOS Keychain, Windows Credential\n"
            "Locker, or Linux Secret Service) — never in a config file.\n"
            "After storing a secret, reference it in config with:\n"
            "    token_secret = keyring:SERVICE/USERNAME"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_EPILOG,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # set
    p_set = sub.add_parser(
        "set",
        help="Store a secret in the OS keychain (prompts for value; nothing is echoed).",
        description=(
            "Store a secret in the OS keychain.\n\n"
            "You will be prompted for the secret value — nothing is echoed to the terminal.\n"
            "After storing, the command prints the exact reference string to paste into config."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Example:
              tabcloud-secrets set tabcloud extractor-pat
              # Then in config/config.ini, [extractor] section:
              #   token_secret = keyring:tabcloud/extractor-pat

              tabcloud-secrets set tabcloud publish-pat
              # Then in config/config.ini, [publish] section:
              #   token_secret = keyring:tabcloud/publish-pat
        """),
    )
    p_set.add_argument("service", help="Service name (e.g. 'tabcloud'). Groups related secrets.")
    p_set.add_argument("username", help="Username / key name within the service (e.g. 'extractor-pat').")
    p_set.set_defaults(func=cmd_set)

    # get
    p_get = sub.add_parser(
        "get",
        help="Retrieve and print a stored secret (output is visible in terminal).",
        description="Retrieve and print a stored secret. Use to verify what is stored.",
    )
    p_get.add_argument("service", help="Service name used when storing.")
    p_get.add_argument("username", help="Username / key name used when storing.")
    p_get.set_defaults(func=cmd_get)

    # delete
    p_del = sub.add_parser(
        "delete",
        help="Remove a stored secret from the OS keychain.",
        description="Remove a stored secret. The secret cannot be recovered after deletion.",
    )
    p_del.add_argument("service", help="Service name used when storing.")
    p_del.add_argument("username", help="Username / key name used when storing.")
    p_del.set_defaults(func=cmd_delete)

    # list
    p_list = sub.add_parser(
        "list",
        help="List usernames stored under SERVICE (not all backends support this).",
        description=(
            "List all usernames stored under SERVICE.\n\n"
            "NOTE: not all keyring backends support this operation. "
            "If listing fails, use 'tabcloud-secrets get' to test individual entries."
        ),
    )
    p_list.add_argument("service", help="Service name to list.")
    p_list.set_defaults(func=cmd_list)

    # doctor
    sub.add_parser(
        "doctor",
        help="Check the active keyring backend and run a round-trip test.",
        description=(
            "Report which keyring backend is active, whether it is a real\n"
            "encrypted secure store, and run a round-trip set/get/delete test.\n\n"
            "Run this after installing keyring or when troubleshooting errors."
        ),
    ).set_defaults(func=cmd_doctor)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
