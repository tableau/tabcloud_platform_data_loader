# Integration / end-to-end tests

These tests run against a **real test tenant** using credentials stored securely
in your OS keychain. They are **opt-in** and **self-skip** when no test
environment is configured, so the default `pytest` run stays green without them.

## One-time setup

### 1. Install keyring support

```bash
pip install -e .[secrets]
```

### 2. Store your test-tenant credentials in the OS keychain

Use a dedicated `tabcloud-test` service name to keep test creds separate from prod:

```bash
tabcloud-secrets set tabcloud-test extractor-pat     # TCM PAT for the test tenant
tabcloud-secrets set tabcloud-test publish-pat       # only if testing the publish path
tabcloud-secrets doctor                              # verify the backend works
```

(You'll be prompted for each value; nothing is echoed to the terminal.)

### 3. Create your test config

```bash
cp config/config.test.ini.example config/config.test.ini
```

Then edit `config/config.test.ini` and fill in your test tenant's `server_url`,
`token_name`, and `site_uris`. The `token_secret` fields already reference the
keyring entries you created above — **no secrets go in the file**.

`config/config.test.ini` is gitignored (it names your real tenant); only the
`.example` template is committed.

## Running

```bash
# Default: unit tests only — integration tests are excluded automatically.
pytest

# Run ONLY integration tests (requires the setup above):
pytest -m integration -v

# Just the fast connectivity smoke tests (verify auth works):
pytest -m integration -k connectivity -v

# Full end-to-end pipeline (extractor → transformer → loader):
pytest -m integration -k e2e -v -s

# Run EVERYTHING (unit + integration), ignoring the default filter:
pytest -m ""

# Point at a different config (e.g. a second tenant):
TABCLOUD_TEST_CONFIG=~/other_tenant.ini pytest -m integration
```

## What's here

| File | Purpose |
|------|---------|
| `conftest.py` | Fixtures: `extractor_credentials` (resolves the PAT via `common.secrets`), `temp_storage`, `runner_env`. All derive from `test_config`, so they skip when no config is present. |
| `test_connectivity.py` | Fast smoke tests — authenticate and list sites against the test tenant. |
| `test_e2e_pipeline.py` | Drives `python -m runner --config config.test.ini` as a subprocess; asserts a `.hyper` is produced and that the secret never leaks into output. |
| `mappings/` | Custom transformer + loader YAML scoped to the test tenant (small entity set, test-specific output names). Edit these for new use cases. |

## Writing new E2E use cases

1. Add or edit a transformer mapping in `mappings/` (mirror the production ones in
   `mappings/transformer/`).
2. Reference it from `mappings/test_loader_bundle.yml` (a new `tables:` entry) or
   create a new loader bundle.
3. Point `config/config.test.ini` `[mappings]` at your new files.
4. Add a test in `test_e2e_pipeline.py` (or a new `test_*.py`) that runs the runner
   and asserts on the output. Mark it with `pytestmark = pytest.mark.integration`.

## CI / headless servers

Keyring is usually unavailable on CI runners. Switch the references in your CI
config to environment variables and set them as CI secrets:

```ini
# config.test.ini (CI variant)
[extractor]
token_secret = env:TABCLOUD_TEST_EXTRACTOR_PAT
```

The same `common.secrets.resolve_secret()` call resolves `env:` and `keyring:`
identically, so no test code changes are needed.
