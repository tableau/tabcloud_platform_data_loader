"""Storage configuration loading and backend factory.

Credential providers for the S3 backend (``credentials.provider`` in storage YAML):

  aws_default_chain    — boto3/s3fs default credential chain (env, ~/.aws, instance role).
                         No static credentials needed.  Recommended for EC2/ECS/Lambda.
  aws_profile          — named AWS profile from ~/.aws/credentials.
  aws_secrets_manager  — fetch static key+secret from AWS Secrets Manager.
  static               — access_key / secret_key specified as secret references.
                         Accepts any scheme supported by common.secrets:
                           keyring:tabcloud/aws-access-key  (recommended local option)
                           env:AWS_ACCESS_KEY_ID
                           file:/path/to/key
                           awssm:my/secret#AWS_ACCESS_KEY_ID
                           plain:AKIAIOSFODNN7EXAMPLE

Quick start for local development using keyring:
  pip install -e .[secrets]
  tabcloud-secrets set tabcloud aws-access-key
  tabcloud-secrets set tabcloud aws-secret-key
  # In storage.yaml:
  #   credentials:
  #     provider: static
  #     access_key: keyring:tabcloud/aws-access-key
  #     secret_key: keyring:tabcloud/aws-secret-key
"""

import logging

from storage.base import StorageBackend, DEFAULT_STORAGE_BASE

logger = logging.getLogger("storage.config")

_s3_experimental_warned = False


def load_storage_config(config_path: str) -> dict:
    """Load and validate a storage config YAML file.

    Returns the parsed dict.  Raises ``ValueError`` for basic schema issues.
    """
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required for storage config. Install with: pip install pyyaml")

    with open(config_path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg, dict):
        raise ValueError(f"Storage config must be a YAML mapping, got {type(cfg).__name__}")

    version = cfg.get("version")
    if version is not None and version != 1:
        raise ValueError(f"Unsupported storage config version: {version}")

    backend = cfg.get("backend", "local")
    if backend not in ("local", "s3"):
        raise ValueError(f"Unknown storage backend: {backend!r}")

    if backend == "s3":
        global _s3_experimental_warned
        if not _s3_experimental_warned:
            logger.warning(
                "S3 storage backend is EXPERIMENTAL and not yet fully tested. "
                "Local storage is the supported path. Known limitation: temporary/STS "
                "credentials (ASIA* keys needing a session token) are not yet supported "
                "by the 'static' provider; use 'aws_default_chain' or 'aws_profile'. "
                "Proceeding anyway."
            )
            _s3_experimental_warned = True

    return cfg


def create_storage_backend(
    config: dict | None = None,
    storage_path: str | None = None,
) -> StorageBackend:
    """Create a :class:`StorageBackend` from *config* or fall back to local.

    Parameters
    ----------
    config:
        Parsed storage config dict (e.g. from :func:`load_storage_config`).
        When *None*, a :class:`~storage.local.LocalStorage` is created using
        *storage_path*.
    storage_path:
        Filesystem path used as the local base when *config* is ``None`` or
        when the config selects the ``local`` backend without an explicit path.
    """
    if config is None:
        from storage.local import LocalStorage
        return LocalStorage(storage_path or DEFAULT_STORAGE_BASE)

    backend = config.get("backend", "local")

    if backend == "local":
        from storage.local import LocalStorage
        path = (config.get("local") or {}).get("path") or storage_path or DEFAULT_STORAGE_BASE
        return LocalStorage(path)

    if backend == "s3":
        return _create_s3_backend(config)

    raise ValueError(f"Unknown storage backend: {backend!r}")


# ---------------------------------------------------------------------------
# S3 construction helpers
# ---------------------------------------------------------------------------


def _s3_extras_from_mapping(s3_cfg: dict) -> dict:
    """Build kwargs for :class:`storage.s3.S3Storage` from optional ``s3`` YAML (config, s3fs)."""
    extra = {}
    if "config_kwargs" in s3_cfg:
        extra["config_kwargs"] = s3_cfg["config_kwargs"]
    s3fs_block = s3_cfg.get("s3fs")
    if isinstance(s3fs_block, dict):
        extra.update(s3fs_block)
    return extra


def _create_s3_backend(config: dict) -> StorageBackend:
    s3_cfg = config.get("s3") or {}
    bucket = s3_cfg.get("bucket")
    if not bucket:
        raise ValueError("S3 storage config requires 's3.bucket'")

    prefix = s3_cfg.get("prefix", "")
    region = s3_cfg.get("region")
    endpoint_url = s3_cfg.get("endpoint_url")

    creds = _resolve_credentials(config.get("credentials") or {})
    s3_opts = {**creds, **_s3_extras_from_mapping(s3_cfg)}

    from storage.s3 import S3Storage
    return S3Storage(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint_url=endpoint_url,
        **s3_opts,
    )


def create_component_backend(
    config: dict | None = None,
    storage_path: str | None = None,
    component: str = "extract",
) -> StorageBackend:
    """Create a :class:`StorageBackend` scoped to a component subfolder.

    Parameters
    ----------
    config:
        Parsed storage config dict (from :func:`load_storage_config`).
        When *None*, falls back to local storage at *storage_path*.
    storage_path:
        Filesystem base path (used when *config* is ``None`` or local).
    component:
        One of ``"extract"``, ``"transform"``, ``"loader"``.
    """
    from storage.base import get_extract_path, get_transform_path, get_loader_path

    _path_fn = {
        "extract": get_extract_path,
        "transform": get_transform_path,
        "loader": get_loader_path,
    }
    path_fn = _path_fn.get(component)
    if path_fn is None:
        raise ValueError(f"Unknown component: {component!r}")

    if config is None:
        from storage.local import LocalStorage
        return LocalStorage(path_fn(storage_path))

    backend = config.get("backend", "local")

    if backend == "local":
        from storage.local import LocalStorage
        base = (config.get("local") or {}).get("path") or storage_path or DEFAULT_STORAGE_BASE
        return LocalStorage(path_fn(base))

    if backend == "s3":
        s3_cfg = config.get("s3") or {}
        bucket = s3_cfg.get("bucket")
        if not bucket:
            raise ValueError("S3 storage config requires 's3.bucket'")
        base_prefix = s3_cfg.get("prefix", "").strip("/")
        component_prefix = f"{base_prefix}/{component}" if base_prefix else component
        creds = _resolve_credentials(config.get("credentials") or {})
        s3_opts = {**creds, **_s3_extras_from_mapping(s3_cfg)}
        from storage.s3 import S3Storage
        return S3Storage(
            bucket=bucket,
            prefix=component_prefix,
            region=s3_cfg.get("region"),
            endpoint_url=s3_cfg.get("endpoint_url"),
            **s3_opts,
        )

    raise ValueError(f"Unknown backend: {backend!r}")


def _resolve_credentials(creds: dict) -> dict:
    """Return extra kwargs for :class:`s3fs.S3FileSystem` based on the
    credential provider specified in the config.

    Supported providers:
      aws_default_chain    — no static creds; uses boto3/s3fs default chain.
      aws_profile          — named AWS profile.
      aws_secrets_manager  — fetch key+secret from AWS Secrets Manager (legacy path).
      static               — access_key / secret_key resolved via common.secrets
                             (supports keyring:, env:, file:, awssm:, plain: references).
    """
    provider = creds.get("provider", "aws_default_chain")

    if provider == "aws_default_chain":
        return {}

    if provider == "aws_profile":
        profile = creds.get("profile")
        if not profile:
            raise ValueError("aws_profile credential provider requires 'credentials.profile'")
        return {"profile": profile}

    if provider == "aws_secrets_manager":
        # Legacy path: keep working but delegate to the shared awssm provider.
        return _resolve_secrets_manager(creds.get("secrets_manager") or {})

    if provider == "static":
        return _resolve_static_credentials(creds)

    raise ValueError(
        f"Unknown credential provider: {provider!r}. "
        f"Valid providers: aws_default_chain, aws_profile, aws_secrets_manager, static."
    )


def _resolve_static_credentials(creds: dict) -> dict:
    """Resolve static access_key / secret_key via the common.secrets resolver.

    Both values may be secret references (keyring:, env:, file:, awssm:, plain:)
    or bare literal strings.  This is the recommended path for local development
    when you want to store S3 credentials in the OS keychain.

    Example storage YAML:
      credentials:
        provider: static
        access_key: keyring:tabcloud/aws-access-key
        secret_key: keyring:tabcloud/aws-secret-key

    Quick start:
      pip install -e .[secrets]
      tabcloud-secrets set tabcloud aws-access-key
      tabcloud-secrets set tabcloud aws-secret-key
    """
    from common.secrets import resolve_secret, SecretResolutionError

    access_key_ref = creds.get("access_key", "")
    secret_key_ref = creds.get("secret_key", "")

    if not access_key_ref or not secret_key_ref:
        raise ValueError(
            "Static credential provider requires 'credentials.access_key' and "
            "'credentials.secret_key'. Both may be secret references "
            "(e.g. keyring:tabcloud/aws-access-key) or plaintext values."
        )

    try:
        access_key = resolve_secret(access_key_ref)
    except SecretResolutionError as exc:
        raise ValueError(f"Could not resolve S3 access_key: {exc}") from exc

    try:
        secret_key = resolve_secret(secret_key_ref)
    except SecretResolutionError as exc:
        raise ValueError(f"Could not resolve S3 secret_key: {exc}") from exc

    return {"key": access_key, "secret": secret_key}


def _resolve_secrets_manager(sm_cfg: dict) -> dict:
    """Fetch S3 credentials from AWS Secrets Manager (legacy provider).

    Delegates to the shared awssm provider in common.secrets for consistency.
    Returns static *key* and *secret*; they do not auto-refresh.  For short-lived
    role credentials (STS), prefer ``aws_default_chain`` (instance role, env, profile)
    or ``aws_profile`` with an assumed role so boto3/s3fs can refresh the session.
    """
    from common.secrets import resolve_secret, SecretResolutionError

    secret_id = sm_cfg.get("secret_id")
    if not secret_id:
        raise ValueError("aws_secrets_manager requires 'credentials.secrets_manager.secret_id'")

    key_map = sm_cfg.get("key_map", {})
    ak_key = key_map.get("access_key", "AWS_ACCESS_KEY_ID")
    sk_key = key_map.get("secret_key", "AWS_SECRET_ACCESS_KEY")

    # Resolve the whole secret via the shared awssm provider.
    try:
        access_key = resolve_secret(f"awssm:{secret_id}#{ak_key}")
    except SecretResolutionError as exc:
        raise ValueError(f"Could not fetch S3 access key from Secrets Manager: {exc}") from exc

    try:
        secret_key = resolve_secret(f"awssm:{secret_id}#{sk_key}")
    except SecretResolutionError as exc:
        raise ValueError(f"Could not fetch S3 secret key from Secrets Manager: {exc}") from exc

    return {"key": access_key, "secret": secret_key}
