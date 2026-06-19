# Ideas & backlog

Track future ideas and priorities for the Platform Data API (extractor, transformer, loader).

---

## High priority

- [ ] **Interactive setup wizard** — if no config file is provided (or `--setup` flag passed), walk the user through the bare-minimum parameters interactively, then write a `config.ini` and confirm what will be created before saving. Based on `config.ini.example` prompts. Should cover: server URL, PAT name/secret, site scope, storage backend (local vs S3), and optionally publish settings.

- [ ] **S3 connectivity debugging** — ASIA-prefix STS keys require a session token in addition to access key + secret key. The static credential provider in `storage/config.py` only stores two fields; need to either (a) add optional `session_token` field wired through to s3fs `token=` kwarg, or (b) support `aws_default_chain` with env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) as the recommended STS path. Blocking full end-to-end S3 test.

- [x] **Snapshot gap warning "(any site)" is misleading** — fixed: gap check was skipping entries whose path lacked date segments, so `newest` stayed empty and every entity type was falsely flagged missing. Decoupled presence tracking from date parsing in `_check_delivery_gaps`.

- [x] **Tenant-wide site reference list** — `tenant_sites` table (schema: reference). Source: TCM REST `GET /api/v1/tenants/{tenantId}/sites` (paginated JSON). Extractor writes raw JSON pages to `extract/reference/tenant_sites/pulled=<TS>/`. Transformer (`tenant_reference_auto.yaml`) normalizes to `transform/reference/tenant_sites.parquet` with `siteLuid` (alias of `siteUUID`) as the join key. Loader loads as a snapshot table (drop+recreate on new pull). Join to ActivityLog: `tenant_sites.siteLuid = ActivityLog.siteLuid`.

- [x] **Tenant-wide user reference list** — `tenant_users` table (schema: reference), membership grain (one row per userLuid x siteLuid). Source: TCM REST `GET /api/v1/tenants/{tenantId}/users` (synchronous, returns `siteRoles[]`/`linkRoles[]`). Extractor paginates and writes raw JSON to `extract/reference/tenant_users/pulled=<TS>/`. Transformer expands `siteRoles[]`, aliases `userId`→`userLuid` and `siteId`→`siteLuid`, serializes `linkRoles[]` as JSON string, retains all attributes. Join to ActivityLog: `tenant_users.userLuid = ActivityLog.actorUserLuid` (+ `siteLuid` for site context). Enable via `reference_entities = tenant_sites,tenant_users` in `[extractor]` and `tenant_reference_auto.yaml` in `transformer_mappings`.

- [ ] **Include site LUID as a column in all entity snapshot tables** — entity snapshots currently don't carry a site identifier in the data itself; the site context is only in the file path. Need to inject `site_luid` (or `site_id`) as a derived column during the transformer/loader stage so cross-site queries and joins are possible without path parsing. Especially important once tenant-wide site and user reference lists are available.

- [ ] Fully test end to end pass with background jobs to Tableau Cloud
-- [X] Run exporter to Hyper database
-- [X] Create a test .tds and run loader without --ensure-upstream - see if it works with a full replace on initial publish!
-- [X] Run it again and see if it works again (replace again).
-- [X] Run it again as incremental - did it work? probably won't!
-- [X] Fix the path confusion. Where are input files, and where are outputs? Are they folders or files, or full paths to files, or patterns?
-- [X] add a real background jobs mapping.
-- [X] What is pipeline_name in the transformer mapping? → renamed to output_filename
-- [] "Blessed" mappings folder - store my background jobs mapping! how can I auto-restrict to a specific site? maybe a placeholder?
-- [X] still have weird stuff with export in names. check to update to load.
-- [] multi-table hyper -- how do we define soft keys and table relationships? what about multi-fact? how many tables can I put in there? What about separate schemas?
-- [ ] Run the upstream extractor and transformer manually to collect new jobs, then run incremental - test to see that it incremented.
-- [ ] Run the full stack with --ensure upstream to see if that works.
-- [ ] Propagate incremental dates through out the pipeline - should include filepath, and field data should always include that plus eventProcessedTime, and include a derived hour date from the folder structure.
-- [] Add Entity Snapshot tables!

---

## Medium / later

- [X] Still says export but needs renaming to loader - missed several things
- [ ] Harden security - no secrets in config files, provide a way to securely retrieve them. Ensure encryption.
- [ ] Error handling and logging - enhance
- [X] Multi-threaded downloads (--download-threads parameter added to extractor)
- [ ] Implement max retention policy - sliding window in destination data

---

## Backlog / maybe

- [ ] Add a UX experience to make this user-friendly
- [ ] Set up a recurring job with scheduler, chron job
- [ ] consider a sort order parameter that defaults to earliest first so timely results begin appearing before older, less relevant events are processed

---

*Add items as bullet points with `- [ ]` for unchecked or `- [x]` when done. Move items between sections as priorities change.*
