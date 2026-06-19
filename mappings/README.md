# Mappings

Mapping configs are organized by stage:

- **transformer/** — Pipeline configs for the Transformer (extracted files → Parquet/CSV). One YAML per pipeline.
- **loader/** — Loader mapping configs for the Loader (Parquet → Hyper and optional Tableau publish).

**Storage and tenant scope**: Use one storage base path per Tableau Cloud tenant. Transformer `folder_pattern` and `file_pattern` discover `eventType=...` or `entityType=...` under `extract/` for both **date-first** and **schema-first** layouts (see extractor `--extract-path-layout`). Do not ad-hoc rearrange `extract/`. Use separate storage paths and mapping files per tenant when processing multiple tenants.

## Transformer mappings (`mappings/transformer/`)

Use-case configs for the Transformer. Each YAML file defines **one pipeline**. The transformer CLI accepts `--mapping` multiple times to process multiple pipelines (e.g. activity logs and snapshots) in a single invocation.

### Activity log mappings

Each YAML defines a single output schema and multiple inputs. **One mapping file produces one output file** (or one partitioned dataset): all JSON logs from every `inputs` entry are **unioned across eventTypes** into a single table, then written in the chosen format.

**Key fields:**

- **output_filename**: Output filename stem (e.g. `unified_event_logs.parquet`).
- **output**: Output definition.
  - **format**: `"parquet"` or `"csv"`. Default is `"parquet"`.
  - **target_path**: Directory for output (relative to storage base or absolute). Default: `{storage-path}/transform`.
  - **partition_by**: Optional list of column names to partition by. Omit or `[]` for a **single merged file**.
  - **schema**: Master schema (list of `field` + `type`). All inputs map into these columns.
- **inputs**: List of input definitions. Each has:
  - **eventType**: Event type name (must match `eventType=` in extract paths).
  - **source**: **folder_pattern**, **file_pattern**, **recursive**.
  - **mappings**: Map each **output schema field** to a JSON path or `"$eventType"`.

### Snapshot mappings

Snapshot mappings use a passthrough mode — Parquet files from `extract/` are copied directly to `transform/` without parsing:

- **passthrough: true** — skip JSON parsing; copy Parquet as-is (fast, lossless).
- **select: latest_per_partition** — before copying, filter to only the newest file per `entityType` value. (Use `all` to retain all historical snapshots for future SCD pipelines; `all_snapshots` is stubbed for future use.)
- **split_by: entityType** — write one output Parquet per entity type (e.g. `data_connections.parquet`, `users.parquet`). The output directory is set by `output.target_path`. **Note:** output filenames match the API's snake_case `entityType=` segments exactly — they are not normalized or converted to PascalCase.
- **partition_field: entityType** — the path segment key used for grouping files.
- A `_manifest.json` sidecar is written to the output directory containing per-entity snapshot timestamps for the loader to consume.

**Example** (`entity_snapshot_auto.yaml`):

```yaml
output_filename: entity_snapshot
output:
  format: parquet
  target_path: "{storage_path}/transform/snapshots"
  passthrough: true
  split_by: entityType
select: latest_per_partition
partition_field: entityType
inputs:
  - source:
      folder_pattern: "entityType=*"
      file_pattern: "*.parquet"
      recursive: true
```

### Running the transformer

```bash
# Activity logs only
python -m transformer --storage-path ./platform_data --mapping mappings/transformer/generic_activity_log_auto.yaml

# Snapshots only
python -m transformer --storage-path ./platform_data --mapping mappings/transformer/entity_snapshot_auto.yaml

# Both in one invocation (default runner behavior)
python -m transformer --storage-path ./platform_data \
  --mapping mappings/transformer/generic_activity_log_auto.yaml \
  --mapping mappings/transformer/entity_snapshot_auto.yaml
```

## Loader mappings (`mappings/loader/`)

YAML files that define the load target (e.g. Hyper file path), tables, and per-table `write_mode`. Optional `schema` per table (Hyper schema name; defaults to `"Extract"`). Optional `target.publish` and `upstream.extractor` sections.

Start from `mappings/loader/template.yml` — every attribute and every valid value is documented inline.

### write_mode — data strategy and schema-drift policy

`write_mode` is a per-table setting that encodes **two behaviors in one key**: the data update strategy and what the loader does when the incoming schema differs from the existing Hyper table.

| `write_mode`     | Data strategy | Schema drift policy |
|---|---|---|
| `replace`        | Rewrite all rows every run | **Allow** — accepts changes silently |
| `strict_replace` | Rewrite all rows every run | **Fail** — ERROR before writing; Hyper table preserved |
| `append`         | Insert rows where `increment_field` > last max | **Rebuild** — drops + rewrites table on drift; logs warning |
| `strict_append`  | Insert rows where `increment_field` > last max | **Fail** — ERROR on drift; watermark not advanced |
| `snapshot`       | Replace only when a newer snapshot arrives (watermark) | **Allow** — accepts changes silently |
| `strict_snapshot`| Replace only when a newer snapshot arrives (watermark) | **Fail** — ERROR on drift; watermark not advanced |

Convention: **plain modes** (`replace`, `append`, `snapshot`) auto-adapt to schema changes. **`strict_*` modes** fail loudly so drift gets operator attention.

**Required per-table fields:**

- `table` — Hyper table name (must match the manifest key for snapshot tables).
- `schema` — Hyper schema name (default `"Extract"`; use `"snapshots"` for snapshot tables by convention).
- `write_mode` — one of the six values above (default: `replace` when omitted).
- `increment_field` — required for `append` / `strict_append`. Column used as incremental high-water mark.
- `transformer_mappings` or `source_file_path` — exactly one required for all non-snapshot tables (mutually exclusive).
- `snapshot_source_dir` — required for `snapshot` / `strict_snapshot`. Directory containing `_manifest.json` and per-entity Parquet files. Supports `{storage_path}` placeholder. Mutually exclusive with `transformer_mappings` and `source_file_path`.

### Snapshot write_mode details

For `write_mode: snapshot` or `write_mode: strict_snapshot`:

1. Loader reads `_manifest.json` from `snapshot_source_dir` to get the per-entity snapshot timestamp.
2. Compares the timestamp to the watermark stored in `_load_state.json`.
3. Performs a full table replace **only if the snapshot is newer**; skips otherwise.
4. For `strict_snapshot`: probes the existing Hyper table schema before writing. If columns changed, raises a `RuntimeError` and does **not** advance the watermark (so the next run retries the same snapshot and fails again until resolved).
5. Updates the watermark after a successful replace.

Snapshot tables and event tables can coexist in a single Hyper data source. No explicit relationship keys are required; analysts define joins in Tableau downstream.

### Publish section

When publishing to Tableau Server/Cloud: `server_url`, `token_name`, `token_secret`, optional `site`, `project` (use `/` for nested project path, e.g. `Analytics/Logs`), `name` (data source name; alias `datasource_name`), `description`, `tds_path` (optional path to .tds — if set, a .tdsx is built and published; otherwise the .hyper is published directly), and `mode`:

| `mode`         | Behavior |
|---|---|
| `upsert`       | (default) PATCH existing datasource; full publish on first run or if PATCH fails |
| `hyper_update` | PATCH data only (requires live-to-Hyper connection on server); no fallback on subsequent runs |
| `overwrite`    | Full publish every run; always drops and recreates |

`mode` is independent of `write_mode` — it controls how Tableau Server is updated, not how the local `.hyper` file is built.

### Example: combined event + snapshot bundle

```bash
python -m loader --storage-path ./data_workspace --loader-mapping mappings/loader/platform_data_bundle.yml
```

With upstream (extractor + transformer first):

```bash
python -m loader --storage-path ./data_workspace --loader-mapping mappings/loader/platform_data_bundle.yml \
  --ensure-upstream --pat-name ... --pat-secret ... --api-url ...
```

See `mappings/loader/platform_data_bundle.yml` for the canonical combined example. See `mappings/loader/template.yml` for a fully annotated reference covering every attribute.
