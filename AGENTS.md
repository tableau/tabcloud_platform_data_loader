# Context for development and AI (Platform Data API)

Use this file to understand the project and where to put or find information. When editing code or docs, prefer patterns and conventions described here.

## What this project is

- **Goal**: Retrieve and replicate Tableau Cloud Manager (TCM) platform data — activity logs and entity snapshots — from the TCM API, transform them into tabular Parquet or CSV using mapping configs, and optionally load Parquet to a database or data warehouse.
- **Three components** (each runnable via CLI independently):
  1. **Extractor** (`python -m extractor`): PAT login, fetch activity log or entity snapshot file lists, download and store files to storage (default: `{storage-path}/extract`). Default: `--dataset both` retrieves both activity logs and snapshots; tenant logs excluded by default.
  2. **Transformer** (`python -m transformer`): Read NDJSON or Parquet from `extract/`, apply one or more mapping YAMLs (see `mappings/transformer/`), write Parquet or CSV to `transform/`. Accepts `--mapping` multiple times for a single invocation. Snapshots use passthrough (Parquet copy) by default with `split_by` per entity type and a `_manifest.json` sidecar.
  3. **Loader** (`python -m loader`): Read Parquet from `transform/`, query DB state, load into target; may write temp files to `loader/`. Per-table `write_mode` controls the data strategy and schema-drift policy. Loader mappings in `mappings/loader/`.

## Storage layout

All components default to the same **storage base path** (default `./platform_data`), each with a dedicated subfolder:

| Folder      | Used by     | Purpose |
| ----------- | ----------- | ------- |
| `extract/`  | Extractor   | Files from API (**date-first** or **schema-first** layout; see `--extract-path-layout`). |
| `transform/` | Transformer | Reads from `extract/`; writes one Parquet or CSV file (or partitioned dir) per pipeline; format in mapping (`output.format`). |
| `loader/`   | Loader      | Temp/staging files if needed. |

CLI args: each component accepts `--storage-path` (default `./platform_data`). Extractor writes to `{storage-path}/extract`; Transformer reads `extract/` and writes to `transform/`; Loader reads `transform/` and may use `loader/`.

### Multi-backend storage

Storage can be local disk (default) or S3. Use `--storage-config` with a YAML file to configure:

```yaml
version: 1
backend: s3   # or local
s3:
  bucket: my-org-platform-data
  prefix: tenants/acme/
  region: us-east-1
credentials:
  provider: aws_default_chain   # or aws_profile / aws_secrets_manager
```

When no `--storage-config` is provided, `--storage-path` is used with local disk (backward compatible). All components (extractor, transformer, loader) accept `--storage-config`. The single-tenant-per-storage-base rule applies to S3 prefixes as well.

Credentials are never stored in the config YAML. Use the AWS default credential chain (recommended), a named profile, or AWS Secrets Manager.

### Storage path structure (must be maintained)

The storage folder path structure is **highly structured**. If that structure is not maintained, **unpredictable behavior can result and should be avoided**.

- **Extractor**: It stores files in one of two **supported** layouts. **date-first** mirrors the API: `y=`/`m=`/`d=`/`h=` then `eventType=...`. **schema-first** moves `eventType=...` before the date block. Use `--extract-path-layout` to choose; omitted means auto-detect from existing `extract/` or default **date-first**. The presigned-URL request still uses the API’s path; only local on-disk names reorder for schema-first.
- **Transformer**: Folder patterns (e.g. `folder_pattern: "eventType=Login"`, which matches at any depth) work with both layouts. Do not hand-edit `extract/` into a third, ad-hoc shape.

Do not reorganize or flatten paths under `extract/` outside the two supported layouts; keep layout consistent for the transformer.

### Single tenant per storage base

A given **storage base path** (and thus a given `--storage-path`) should be used for **one Tableau Cloud tenant only**. Do not span multiple tenants in the same storage tree.

- **Path structure**: The layout is tenant-specific; co-mingling data from multiple tenants in one tree increases the risk of path collisions or ambiguous structure.
- **Data safety**: Co-mingling multi-tenant data in one storage (and thus in one set of transform/load outputs) creates a much greater risk of accidentally publishing or loading data to an end repository that mixes multi-tenant information, leading to cross-pollination of data.
- **Intended use**: The utility is designed to unify data **between sites on a single tenant** and to publish (or load) results to a single site or target within that tenant.
- **Authentication**: Authentication is designed to accept **one PAT (token)** for one tenant. To pull data from multiple tenants, use **separate storage base paths** (e.g. different `--storage-path` per tenant) and **separate mapping files** for each tenant’s pipeline and loader configs.

## Key concepts

- **TCM API (Tableau Cloud Manager)**: Base URL (`--api-url`). PAT login, activity log, and entity snapshot endpoints live under this base.
- **PAT**: Personal Access Token: `--pat-name` and `--pat-secret`. Login POST sends `token: pat_secret`; response has `sessionToken`, `tenantId`.
- **Tenant vs site logs**: Tenant-level logs: `GET/POST .../api/v1/tenants/{tenantId}/activitylog`. Site-level logs: `GET/POST .../tenants/{tenantId}/sites/{siteId}/activitylog`. `--retrieve-tenant-logs` (default `false`) and `--site-ids` control scope. Snapshots are **site-only** (no tenant-level endpoint); `--allow-tenant-snapshots` is an escape hatch.
- **Activity log list**: GET with `startTime`, `endTime`, `pageSize`, `pageToken`; response has `files` (each with `path`). Paths encode `eventType=...`.
- **Entity snapshots**: Point-in-time full dumps of Tableau Cloud internal entities (Users, Groups, Workbooks, etc.), available on an **hourly** cadence for Enterprise/Tableau+ customers. Retrieved from `.../snapshots` with `entityType` filter. Paths encode `entityType=...`. Files are already **Parquet** (no parsing needed). Available for Enterprise and Tableau+ editions only (Standard customers will get a 403 or empty response).
- **DatasetProfile**: `src/common/dataset_profile.py`. The `DatasetProfile` dataclass (`name`, `resource`, `filter_param`, `path_key`, `is_site_only`, `files_are_parquet`) abstracts differences between `ACTIVITYLOG` and `SNAPSHOTS`. Use `get_profile("activitylog"|"snapshots")`. All extractor path, presence, and retriever logic is parameterized through this profile.
- **Snapshot selection**: `--snapshot-selection latest` (default) keeps only the newest file per entity type in local storage. `--snapshot-selection all` retains all historical snapshots (useful for downstream SCD pipelines).
- **Cadence-aware lookback**: Extractor defaults to `--snapshot-cadence hourly` with a `--snapshot-lookback-intervals 4` window (4 hours back) so snapshots that arrive late within their hour are captured. Activity logs default to a 2-day lookback.
- **Delivery-gap detection**: After selecting files, the extractor compares expected entity types against received files and warns/errors/ignores based on `--snapshot-gap-action`.
- **Storage**: The `storage/` package provides `StorageBackend` (ABC), `LocalStorage`, `S3Storage`, `create_storage_backend`, `create_component_backend`, `load_storage_config`, `get_extract_path()`, `get_transform_path()`, `get_parquet_path()` (alias), `get_loader_path()`, and `DEFAULT_STORAGE_BASE`. Extractor uses `create_component_backend(config, storage_path, "extract")` or `LocalStorage(get_extract_path(storage_path))` for local-only use. `extract_segment_value_from_path(path, key)` is the generic path parser (supersedes `extract_event_type_from_path`).
- **Mappings**: `mappings/transformer/` — pipeline YAML per file (output schema, inputs). `mappings/loader/` — loader mapping YAML (target, tables). See `mappings/README.md`.
- **Transformer mapping for snapshots**: Supports `passthrough: true` (skip JSON parsing; copy Parquet as-is), `split_by: entityType` (one output file per entity type value), `select: latest_per_partition` (keep only the newest snapshot per entity type), and `partition_field: entityType`. A `_manifest.json` sidecar is written with per-entity snapshot timestamps for the loader.
- **Loader mapping**: YAML with `target` (the .hyper file path), `tables` (each table: `table`, `write_mode`, and **either** `transformer_mappings`, `source_file_path`, or `snapshot_source_dir`—mutually exclusive). `write_mode` encodes both the data strategy and schema-drift policy: `replace`/`strict_replace` — full table replace every run, allow/fail on drift; `append`/`strict_append` — incremental insert by `increment_field`, rebuild/fail on drift; `snapshot`/`strict_snapshot` — replace-on-new-snapshot (watermarked in `_load_state.json`), allow/fail on drift. Plain modes auto-adapt; `strict_*` modes fail loudly. For `append`/`strict_append`, `increment_field` is required. For `snapshot`/`strict_snapshot`, `snapshot_source_dir` is required. `strict_snapshot` does NOT advance the watermark on drift error. See `mappings/loader/template.yml` for a fully annotated reference. Upstream transformer mappings are derived only from tables using `transformer_mappings`. Optional `upstream.extractor` for --ensure-upstream defaults. **Publish** (optional): `target.publish` includes `server_url`, `token_name`, `token_secret`, `name`, `description`, `site`, `project`, `tds_path`, and `mode` (`upsert`|`hyper_update`|`overwrite`). **Two credential sets**: (1) TCM/retriever for `--ensure-upstream`; (2) Tableau Server/Cloud for publishing.
- **Snapshot watermark**: The loader stores the last loaded snapshot timestamp per table in `_load_state.json` (key `{table}_snapshot_time`). On each run it reads the `_manifest.json` sidecar from the transformer and skips the replace if the snapshot has not changed. For `strict_snapshot`, the watermark is not advanced when a drift error is raised.
- **SCD (future)**: Slowly Changing Dimension tracking is explicitly deferred. The `select: all_snapshots` mode is stubbed in the transformer for future use. Historical snapshots can be retained with `--snapshot-selection all`; downstream tools (dbt, warehouse) can build Type-2 history without changes to this tool.
- **Hyper tables**: Event and snapshot tables can be bundled into a single `.hyper` data source without explicitly defined keys. Analysts define relationships in Tableau using the bundled tables. Use `write_mode: append` for event tables and `write_mode: snapshot` for entity tables in one combined loader mapping.
- **Hyper type normalization**: The loader uses **PyArrow tables** throughout (no pandas). Types are normalized in `loader/targets/hyper.py` (`normalize_table_for_hyper`). See [pantab Common Issues](https://pantab.readthedocs.io/en/latest/common_issues.html).
- **Throttling**: On HTTP 429, Extractor sleeps `--wait-time` seconds and retries.
- **Logging**: Each component tees stdout/stderr to a per-run rotating log under `{storage-path}/logs` by default (`--log-dir`, `--log-retention-days`). Structured JSON operations use `log_operation()`. No resumable state file.

## Where things live

| What | Where |
|------|--------|
| Extractor CLI, LogRetriever, API calls | `extractor/` |
| Shared storage backend, config, and folder layout | `storage/` |
| Transformer: reader, mapping, pipeline, Parquet/CSV I/O, CLI | `transformer/` |
| Loader: state query, load logic, CLI | `loader/` |
| Transformer mapping configs (YAML) | `mappings/transformer/` |
| Loader mapping configs (YAML) | `mappings/loader/` |
| Specs, architecture | `docs/` |
| Project overview | `README.md` |
| Cursor rules | `.cursor/rules/` |

## Conventions

- Python: standard library + `requests` (extractor), `pyarrow`, `pyyaml` (transformer, loader). Optional: `fsspec`, `s3fs`, `boto3` (S3 storage backend). Loader uses PyArrow tables only (no pandas). Use patterns in `extractor/`, `transformer/`, `loader/`, and `storage/`.
- Config: CLI args. Extractor: `--pat-name`, `--pat-secret`, `--api-url`, `--start-time`, `--end-time`, `--site-ids`, `--storage-path` (base), `--extract-path-layout` (`date-first` or `schema-first`), `--dataset` (`activitylog`|`snapshots`|`both`; default `both`), `--entity-type`, `--snapshot-selection` (`latest`|`all`), `--snapshot-cadence` (`hourly`|`daily`), `--snapshot-lookback-intervals`, `--snapshot-gap-action` (`warn`|`error`|`ignore`), `--list-entities`, `--allow-tenant-snapshots`, `--retrieve-tenant-logs`/`--no-retrieve-tenant-logs` (default `false`). Transformer: `--storage-path`, `--mapping` (repeatable), `--output`. Loader: `--storage-path`, `--target`, `--mode incremental|bulk`. All components: `--storage-config` (optional YAML for S3/multi-backend storage). Runner: all of the above are configurable via `config.ini` under `[extractor]`.
- When changing API or storage layout, update `docs/architecture.md` and this file.

## Adding context for AI

- **Specs / API details**: Put in `docs/` (e.g. `api-contract.md`, `architecture.md`) and reference with `@docs/...`.
- **OpenAPI spec**: `docs/openapi.yaml` or `docs/openapi.json`.
- **Project-wide rules**: `.cursor/rules/*.mdc`.

Keeping README, AGENTS.md, and `docs/` updated when behavior or APIs change helps both humans and AI stay aligned with the codebase.
