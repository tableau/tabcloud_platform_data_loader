# Project documentation

This folder holds contextual documents that aid development and onboarding.

## Suggested contents

| File | Purpose |
|------|--------|
| **architecture.md** | High-level flow: PAT login → optional List Tenant Sites (for --site-ids All or validation) → list activity log files (tenant and/or per-site) → get presigned URLs → download and replicate via StorageBackend. Throttling (429 + wait_time). No state file; JSON operation log to stdout. |
| **api-contract.md** | TCM (Tableau Cloud Manager) API: base URL, PAT login (`/api/v1/pat/login`), List Tenant Sites (`GET .../tenants/{id}/sites`), activity log list (`GET .../activitylog` or `.../sites/{siteId}/activitylog`), presigned URLs (`POST` with `{"files": [...]}`). Headers: `x-tableau-session-token`. Request/response shapes for `files`, `sites`, pagination. |
| **decisions.md** | ADRs or notes: e.g. pluggable storage, no resumable state, JSON stdout logging, site-ids All/None/UUID list, throttle wait time. |
| **specs/** | Optional subfolder for feature specs or external spec files (e.g. OpenAPI, Postman exports). |

## How this helps

- **Humans**: Single place for how the system works and how the API behaves.
- **AI / Cursor**: Refer to these files with `@docs/` or `@docs/architecture.md` so suggestions stay aligned with your design and API contract.

Keep docs close to the code: update them when behavior or APIs change.
