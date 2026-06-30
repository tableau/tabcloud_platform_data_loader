# Platform Data Loader for Tableau Cloud

Tableau Cloud holds a lot of [useful data about what's happening in your environment](https://help.tableau.com/current/api/cloud-manager/en-us/docs/platform_data.html) — who's logging in, what's being used, and regular snapshots of users, groups, content, and more — and it's all available through an API, though in a raw format that takes some work to turn into something useful. This tool does that work for you: it pulls the data out, cleans it up into simple tables, and (if you want) loads it straight into a ready-to-use Tableau data source — all from a single config file. It's built in three pieces you can run together or on their own, so you can use just the part you need and adapt it to new uses by editing config instead of writing code.

There are three basic parts to this tool:
- **Extractor**: Retrieves raw logs and/or entity snapshots from Tableau Cloud based on date range and/or type, and replicates them to local storage (or S3 — experimental). By default pulls **both** activity logs and entity snapshots; tenant logs excluded by default.
- **Transformer**: Transforms extracted NDJSON logs into unified .csv or .parquet files; copies extracted Parquet snapshots as-is, splitting by entity type and generating a sidecar manifest. Accepts multiple `--mapping` files in one invocation.
- **Loader**: Assembles many "tables" from the Transformer into a single database (.hyper supported now; more types come in the future). Supports incremental event tables and replace-on-new-snapshot entity tables in the same Hyper data source. Publishes to Tableau with an optional .tds semantic layer wrapper.

Repository: [tabcloud_platform_data_loader](https://github.com/tableau/tabcloud_platform_data_loader).

## Quick start

```bash
# 1. Install (with optional keyring support for secure secret storage):
pip install -e ".[secrets]"

# 2. Store your Tableau Cloud Manager PAT in the OS keychain (recommended):
tabcloud-secrets set tabcloud extractor-pat    # prompts for value; nothing echoed
tabcloud-secrets doctor                         # verify the backend

# 3. Run the full pipeline with a single command (interactive setup on first run):
tabcloud-run -c config/run.yml

# 4. Or copy the annotated example and fill in your values:
cp config/run.example.yml config/run.yml
# Edit config/run.yml → set connection.tcm_url, token_name, token_secret
tabcloud-run -c config/run.yml

# 5. Preview what would run without executing anything:
tabcloud-run -c config/run.yml --explain
```

The runner drives the full **extraction → transform → load** pipeline from a
single `run.yml` file.  All three phases run automatically with sensible defaults
when the corresponding blocks are absent from the config.  By default, this
retrieves all event (activity) logs from every site in the tenant along with the
most recent snapshots for each site, and dumps all of that into a single `.hyper`
file as a set of tables.  If you want different behavior, customize `run.yml` —
and each module can be tuned further through the mappings referenced from
`run.yml` (transformer and loader mapping files).  Publishing to Tableau
is opt-in and destination-driven — add a `destinations:` block and reference it
from a `load:` job.

See `config/run.example.yml` for a fully-annotated reference with all options.

### Direct component invocation

Each component can also be run individually:

```bash
# Extract only:
python -m extractor \
  --pat-name <NAME> \
  --pat-secret-ref keyring:tabcloud/extractor-pat \
  --api-url <API_URL>
```

Older plaintext `--pat-secret` is still accepted but deprecated — it appears in the process list (`ps`) and is not recommended.

Optional extractor flags: `--site-uris` (comma-separated contentURLs, `All`, or `None`), `--storage-path`, `--storage-config`, `--dataset` (`activitylog`|`snapshots`|`both`), `--event-type`, `--entity-type`, `--snapshot-selection` (`latest`|`all`), `--snapshot-cadence`, `--snapshot-lookback-intervals`, `--snapshot-gap-action`, `--retrieve-tenant-logs`/`--no-retrieve-tenant-logs` (default off), `--list-entities`, `--wait-time`, `--page-size`.

See `python -m extractor --help` for all options.

## Requirements

- Python 3.9–3.14 (pantab 5.3.0+ includes Python 3.14 support)
- Dependencies: `requests`, `pyarrow`, `pyyaml`, `pantab` (optional: `fsspec`, `s3fs`, `boto3` for S3 storage)

## Setup

```bash
# Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# Install the project (editable) with the S3 storage extra
pip install -e ".[s3]"

# For development (adds pytest)
pip install -e ".[s3,dev]"
```

Dependencies are declared in `pyproject.toml`. Optional extras:
- `.[secrets]` — `keyring>=24` for OS keychain secret storage (recommended).
- `.[s3]` — `fsspec`, `s3fs`, `boto3` for the S3 storage backend.
- `.[dev]` — `pytest` for running tests.

`pip install -r requirements.txt` is equivalent to `pip install -e ".[s3]"`.

> **S3 storage is experimental and not yet fully tested.** Local disk storage is the supported default. Known limitation: temporary/STS credentials (keys with an `ASIA` prefix) require a session token that the `static` credential provider does not yet support — use `aws_default_chain` or `aws_profile` instead. A runtime warning is emitted whenever an S3 storage config is loaded.

## Secrets management

Secrets (PATs, S3 keys) are **never stored in config files** and **never appear in the process list** (`ps`).

### Quick start (recommended: keyring)

```bash
pip install -e .[secrets]
tabcloud-secrets set tabcloud extractor-pat   # store TCM PAT; prompts, nothing echoed
tabcloud-secrets set tabcloud publish-pat     # store Tableau PAT (if publishing)
tabcloud-secrets doctor                        # verify the keyring backend
```

Then in `config/config.ini`:

```ini
[extractor]
token_secret = keyring:tabcloud/extractor-pat

[publish]
token_secret = keyring:tabcloud/publish-pat
```

### Supported secret reference schemes

| Scheme | Example | When to use |
|--------|---------|-------------|
| `keyring:SERVICE/USERNAME` | `keyring:tabcloud/extractor-pat` | Local workstations (macOS/Windows/Linux with Secret Service) |
| `env:VAR_NAME` | `env:MY_PAT` | CI/CD, containers, scripts |
| `file:/path` | `file:/run/secrets/pat` | Containers with mounted secrets |
| `awssm:ID#KEY` | `awssm:tabcloud/creds#PAT` | AWS-hosted deployments |
| `plain:VALUE` | `plain:abc123` | Quick local testing only |

See `docs/architecture.md` and `tabcloud-secrets --help` for details, per-OS notes, and headless-server guidance.

## Project layout

| Path | Purpose |
|------|--------|
| `src/extractor/` | Extractor: PAT login, list tenant sites, activity log and snapshot API (tenant + site), presigned URLs, download, storage backend, JSON operation logging. Run: `python -m extractor` |
| `src/transformer/` | Transformer: extracted NDJSON or Parquet → Parquet/CSV using mappings in `mappings/transformer/`. Supports passthrough+split_by for snapshots. Run: `python -m transformer` |
| `src/loader/` | Loader: Parquet → Hyper (and optional Tableau publish). Per-table `write_mode` controls data strategy (replace/append/snapshot) and schema-drift policy (allow/fail/rebuild). Loader mappings in `mappings/loader/`. Run: `python -m loader` |
| `src/common/` | Shared abstractions: `DatasetProfile` (activitylog / snapshots); `secrets` (unified secret-resolution layer with provider registry) |
| `mappings/transformer/` | Pipeline YAML configs for the transformer (event logs and snapshots) |
| `mappings/loader/` | Loader mapping YAML configs for the loader |
| `src/storage/` | Shared storage backends (local, S3), config, and path helpers |
| `docs/` | Specs, architecture, and API notes (see `docs/README.md`) |

## Storage and tenant scope

- **Path structure**: Storage uses a fixed folder layout (`extract/`, `transform/`, `loader/`). The extractor preserves the **API path structure** under `extract/` (e.g. `eventType=Login/...` for logs, `entityType=Users/...` for snapshots); the transformer relies on it via mapping patterns. Do not alter this structure—unpredictable behavior can result.
- **One tenant per storage**: Use each `--storage-path` (storage base) for **one Tableau Cloud tenant only**. Co-mingling multiple tenants in one base risks cross-tenant data leakage when publishing or loading. To process multiple tenants, use separate storage paths and separate mapping files per tenant. See `AGENTS.md` for details.
- **Entity snapshots**: Available for Enterprise and Tableau+ customers only (Standard customers will receive a 403 or empty response). Snapshots are site-scoped only at launch.

## Running tests

```bash
# Unit tests (default — fast, no credentials needed; integration tests excluded automatically):
pytest

# Integration / end-to-end tests against a real test tenant (opt-in):
pytest -m integration
```

Unit tests mock all I/O and require no credentials. **Integration tests** run the
real extractor → transformer → loader pipeline against a live test tenant using
credentials stored in your OS keychain. They are excluded from the default run and
self-skip when no `config/config.test.ini` is present.

Quick setup for integration tests:

```bash
pip install -e .[secrets]
tabcloud-secrets set tabcloud-test extractor-pat   # store your test-tenant PAT
cp config/config.test.ini.example config/config.test.ini   # then edit tenant/site
pytest -m integration -k connectivity -v           # verify auth works
```

See **[`tests/integration/README.md`](tests/integration/README.md)** for the full
setup, run options, custom-mapping workflow, and CI/headless guidance.

## Documentation

- **Setup and usage**: this README
- **Secrets management**: this README ("Secrets management") + `docs/architecture.md`
- **Integration / E2E testing**: [`tests/integration/README.md`](tests/integration/README.md)
- **Context for development and AI**: `AGENTS.md`
- **Specs and design**: `docs/` (architecture, API contracts, decisions)

## Governance (Salesforce open source template)

This repository includes the standard Salesforce open source templates at the repo root: `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `LICENSE.txt`, `SECURITY.md`, and `CODEOWNERS`. Follow those documents for contributions, licensing, and security reporting.
