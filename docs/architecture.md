# Architecture: Extractor, Transformer, Loader

## Overview

Three components share one storage base path and run independently via CLI:

1. **Extractor** — API → extracted files (`extract/`). Handles two dataset types: activity logs (NDJSON, `eventType=` paths) and entity snapshots (Parquet, `entityType=` paths). Default: `--dataset both`.
2. **Transformer** — extracted NDJSON or Parquet → Parquet or CSV (`transform/` by default) using one or more mapping YAMLs; output format is set by `output.format` in the mapping. Snapshots use a fast passthrough (Parquet copy) with `split_by` per entity type and a `_manifest.json` sidecar.
3. **Loader** — Parquet → database/warehouse (reads `transform/`, may write to `loader/`). Each table has a `write_mode` that encodes both the data strategy and the schema-drift policy (see [Loader mappings](#loader-mappings) below).

## Storage layout

Default base path: `./platform_data` (override with `--storage-path` for each component).

| Path under base | Component   | Purpose |
| ----------------- | ----------- | ------- |
| `extract/`        | Extractor   | Raw files (NDJSON for logs, Parquet for snapshots); **date-first** (API order) or **schema-first** (`--extract-path-layout`). |
| `transform/`      | Transformer | Reads from `extract/`. Output: one Parquet or CSV file (or partitioned directory) per pipeline; snapshot passthrough writes one Parquet per entity type plus `_manifest.json`. |
| `loader/`         | Loader      | Temp or staging files if needed. |

All three default to the same base so that, with no arguments, Extract → Transform → Load use the same tree.

### Storage path structure must be maintained

The storage folder path structure is **highly structured**. If it is not maintained, **unpredictable behavior can result and should be avoided**. The extractor writes under `extract/` in a **date-first** or **schema-first** layout (see extractor CLI). The transformer discovers files using **folder and file patterns** in the mapping YAML (e.g. `folder_pattern: "eventType=Login"` or `folder_pattern: "entityType=Users"`). Ad-hoc edits under `extract/` can cause the transformer to miss files or match incorrectly.

### Single tenant per storage base

A given storage base path should be used for **one Tableau Cloud tenant only**. Do not span multiple tenants in the same storage tree. Reasons: (1) path structure is tenant-specific; (2) co-mingling multi-tenant data greatly increases the risk of accidentally publishing or loading data to a repository that mixes tenants (cross-pollination); (3) the utility is designed to unify data across **sites within one tenant** and publish to a single site on that tenant; (4) authentication accepts one PAT per run (one tenant). To process multiple tenants, use **separate storage base paths** and **separate mapping files** for each tenant.

## Data flow

- **Extractor**: Authenticates with the Tableau Cloud Manager (TCM) API (PAT). A `DatasetProfile` (`src/common/dataset_profile.py`) parameterizes the URL resource, filter parameter, and path segment key for either `ACTIVITYLOG` (NDJSON, `eventType=`) or `SNAPSHOTS` (Parquet, `entityType=`). With `--dataset both` (default), a two-pass run retrieves both types. Snapshot selection (`--snapshot-selection latest|all`) filters per entity type before download. Cadence-aware lookback (`--snapshot-cadence`, `--snapshot-lookback-intervals`) and delivery-gap detection (`--snapshot-gap-action`) guard against missed snapshots. Tenant logs are excluded by default (`--no-retrieve-tenant-logs`). Snapshots are site-only at launch (no tenant-level endpoint). Local path layout is **date-first** (mirrors API) or **schema-first**; list/presigned calls still use API paths.

- **Transformer**: Accepts `--mapping` one or more times; processes each mapping in a single invocation. For activity logs: discovers NDJSON files in `extract/` by folder/file pattern, applies field mappings, unions rows, writes one Parquet/CSV file. For snapshots: `passthrough: true` copies Parquet directly (no parsing); `split_by: entityType` creates one output file per entity type; `select: latest_per_partition` filters to the newest snapshot per partition before copying; a `_manifest.json` sidecar is written alongside with per-entity snapshot timestamps. Both dataset types can be processed in one transformer invocation.

- **Loader**: Lists Parquet files from `transform/`, optionally queries DB state, loads selected Parquet into the target. Per-table `write_mode` controls both the data strategy and schema-drift policy (see table below). For snapshot tables, reads the `_manifest.json` sidecar to get the snapshot timestamp, compares to the watermark in `_load_state.json` (key `{table}_snapshot_time`), and only performs a full table replace when a newer snapshot is detected. Snapshot and event tables can coexist in a single Hyper data source without explicitly defined keys. State contract and load logic are target-specific (`loader/targets/`).

## Running components

- **Extractor** (both datasets, defaults): `python -m extractor --pat-name ... --pat-secret ... --api-url ... --site-ids All`
- **Extractor** (activity logs only, explicit window): `python -m extractor --pat-name ... --pat-secret ... --api-url ... --dataset activitylog --start-time ... --end-time ...`
- **Extractor** (snapshots only): `python -m extractor --pat-name ... --pat-secret ... --api-url ... --dataset snapshots --site-ids All`
- **Transformer** (both datasets in one call): `python -m transformer --storage-path ./platform_data --mapping mappings/transformer/generic_activity_log_auto.yaml --mapping mappings/transformer/entity_snapshot_auto.yaml`
- **Loader**: `python -m loader --storage-path ./platform_data --loader-mapping mappings/loader/platform_data_bundle.yml`

## Mapping config

See `mappings/README.md`. Transformer mappings live in `mappings/transformer/`; loader mappings in `mappings/loader/`.

### Activity log transformer mappings

Each YAML defines one **pipeline** with:

- **output**: Master **schema** (field names and types), **target_path**, **format** (`"parquet"` or `"csv"`), optional **partition_by**.
- **inputs**: List of inputs; each has **eventType**, **source** (folder_pattern, file_pattern, recursive), and **mappings** (output field → JSON path or `$eventType`).

One file (or partitioned directory) is written per pipeline (e.g. `unified_event_logs.parquet`).

### Snapshot transformer mappings

Snapshot mappings use a passthrough mode:

- **passthrough: true** — copies Parquet files without parsing (fast, lossless).
- **select: latest_per_partition** — keeps only the newest file per `entityType` value before copying.
- **split_by: entityType** — one output file per entity type (e.g. `data_connections.parquet`, `users.parquet`).
- **partition_field: entityType** — the path segment key used for partitioning.
- A `_manifest.json` sidecar is written alongside the output files containing per-entity snapshot timestamps.

> **Entity type casing**: `entityType` values are API-defined, case-sensitive, and are not normalized by this tool. The Tableau Cloud Manager API emits snake_case values (`data_connections`, `datasources`, `users`, etc.). Use these exact values in `entity_types` config and loader table mappings. PascalCase examples in older docs/examples are incorrect — use `--list-entity-types` to see the exact values for your tenant.

See `mappings/transformer/entity_snapshot_auto.yaml` for the canonical example.

### Loader mappings

Each loader mapping YAML defines:

- **target**: the `.hyper` file path. Optional `publish` block for Tableau Server/Cloud.
- **tables**: per-table config with `table`, `write_mode`, and exactly one data source.

**`write_mode`** encodes the data update strategy and schema-drift policy in one key:

| `write_mode`     | Data strategy | Schema drift |
|---|---|---|
| `replace`        | Full table replace every run | Allow |
| `strict_replace` | Full table replace every run | Fail (ERROR before write) |
| `append`         | Insert rows where `increment_field` > last max | Rebuild (drop + rewrite on drift) |
| `strict_append`  | Insert rows where `increment_field` > last max | Fail (ERROR; watermark not advanced) |
| `snapshot`       | Replace only when a newer snapshot arrives | Allow |
| `strict_snapshot`| Replace only when a newer snapshot arrives | Fail (ERROR; watermark not advanced) |

Plain modes auto-adapt to schema changes; `strict_*` modes fail loudly.

**Data source** (exactly one, mutually exclusive per table):
  - `transformer_mappings` — links to transformer pipeline output in `transform/`.
  - `source_file_path` — directory of CSV/Parquet files to union.
  - `snapshot_source_dir` — directory containing `_manifest.json` and per-entity Parquet files (required for `snapshot` / `strict_snapshot`).

See `mappings/loader/template.yml` for a fully annotated reference. See `mappings/loader/platform_data_bundle.yml` for a combined event + snapshot example.

## Contracts

- **Extract storage**: Extractor writes relative paths under `extract/` in a supported layout. Activity log paths use an `eventType=` segment; snapshot paths use `entityType=`. `extract_segment_value_from_path(path, key)` in `storage/base.py` is the generic parser; `extract_event_type_from_path()` is a back-compat alias.
- **Parquet/CSV**: Transformer writes to `transform/{table_name}.parquet` (or per-entity Parquet files for snapshots) plus a `_manifest.json` sidecar. Loader reads from `transform/` (Parquet only).
- **Snapshot watermark**: Loader reads `_manifest.json` from `snapshot_source_dir`, compares the `snapshot_time` per entity to the value stored in `_load_state.json`, and replaces the Hyper table only when the snapshot is newer.

## Storage abstraction

The `storage/` package provides a pluggable backend for all file I/O:

- **`StorageBackend`** (ABC): defines `list_files`, `exists`, `read_file`, `read_bytes`, `write_bytes`, `write_file`, `full_path`, `uri`
- **`LocalStorage`**: local filesystem implementation
- **`S3Storage`**: S3-compatible object storage via `s3fs`/`fsspec` (**experimental** — not yet fully tested; temporary/STS credentials with `ASIA*` keys require a session token not yet supported by the `static` provider; use `aws_default_chain` or `aws_profile` for assumed-role or instance credentials)
- **`create_storage_backend(config, storage_path)`**: factory from config dict
- **`create_component_backend(config, storage_path, component)`**: scoped to extract/, transform/, or loader/
- **`extract_segment_value_from_path(path, key)`**: generic `key=value` segment parser for both `eventType=` and `entityType=` paths.

### Configuration

Storage config is a YAML file passed via `--storage-config`. When omitted, local storage at `--storage-path` is used.

### S3 presence validation

For "skip existing" checks on S3, the extractor uses batch `ListObjectsV2` with date-anchored prefixes instead of per-file `HeadObject`. The partition key (`eventType` or `entityType`) is passed through to `_compute_list_prefixes` in `storage/presence.py`:

- **Date-first layout**: one list call per day (or per month for multi-day ranges)
- **Schema-first layout**: one list call per partition value per day (or per month), parallelized

This reduces API calls from O(N files) to O(months × partitionValues) at worst.

## Secrets management

Secrets (PATs, S3 keys) are **never passed as plaintext command-line arguments**.  The runner passes a *secret reference* string such as `keyring:tabcloud/extractor-pat` to each component CLI via `--pat-secret-ref` / `--tableau-token-secret-ref`.  If a bare literal appears in config the runner injects it via a dedicated child environment variable (invisible to `ps`).

### Secret reference syntax

A secret value in config may be a bare literal (back-compat) or a `scheme:locator` reference:

| Scheme | Locator format | Description |
|--------|---------------|-------------|
| `keyring` | `SERVICE/USERNAME` | OS keychain via the `keyring` library — **recommended** |
| `env` | `VAR_NAME` | Read from an environment variable |
| `file` | `/path/to/file` | File contents (trailing newline stripped) |
| `awssm` | `SECRET_ID` or `SECRET_ID#JSON_KEY` | AWS Secrets Manager |
| `plain` | `LITERAL` | Explicit literal (same as a bare string) |

Disambiguation rule: a string is treated as a reference only when the substring before the first `:` exactly matches a registered scheme name and contains no whitespace.  Any other string (including real PATs with `:` in them) is treated as a plain literal.

### Quick start (local development with keyring)

```bash
# 1. Install keyring support (once per environment):
pip install -e .[secrets]

# 2. Store your secrets (you'll be prompted; nothing is echoed):
tabcloud-secrets set tabcloud extractor-pat
tabcloud-secrets set tabcloud publish-pat     # only if publishing to Tableau

# 3. Verify the backend is working:
tabcloud-secrets doctor

# 4. Reference secrets in config/config.ini:
# [extractor]
# token_secret = keyring:tabcloud/extractor-pat
# [publish]
# token_secret = keyring:tabcloud/publish-pat
```

### Per-OS keyring notes

| OS | Backend | Notes |
|----|---------|-------|
| macOS | Keychain | Encrypted, tied to user login. Unlock with Keychain Access.app if needed. |
| Windows | Credential Locker | Encrypted, tied to Windows user account. |
| Linux | Secret Service | Install `gnome-keyring` or `kwallet`. On headless servers use `env:` or `file:` instead. |

### Headless / CI servers

Keyring is not available on most headless servers.  Use:
- `env:VAR_NAME` — set the variable in your CI secrets configuration (GitHub Actions secrets, etc.).
- `file:/path/secret` — write the secret to a 600-permission file mounted into the container.
- `awssm:SECRET_ID#FIELD` — AWS Secrets Manager (needs `.[s3]` extra; instance role recommended).

### Extensibility

Third-party providers (HashiCorp Vault, GCP Secret Manager, Azure Key Vault) can register themselves at startup:

```python
from common.secrets import register_provider

def _my_vault_provider(locator: str) -> str:
    ...  # resolve from Vault

register_provider("vault", _my_vault_provider)
# Config can then use: token_secret = vault:secret/data/tabcloud#pat
```

## SCD / history (future)

Slowly Changing Dimension tracking is explicitly **not built** in this release. Design rationale: snapshots are full dumps with no native change tracking; SCD would require persistent historical state (snapshot store + diff + surrogate keys/effective-dating) which conflicts with the tool's stateless, file-pull design. The `--snapshot-selection all` mode retains every snapshot on disk/S3, allowing a downstream warehouse or dbt project to build Type-2 history without changes to this tool. The `select: all_snapshots` transformer mode is stubbed for future use.
