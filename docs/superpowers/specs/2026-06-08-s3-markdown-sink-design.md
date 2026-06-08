# S3MarkdownSink — wurzel sink for the DT-CZ custom-table KB experiment

**Date:** 2026-06-08
**Status:** design approved, pre-implementation
**Repo:** `telekom/wurzel` (this repo). Ingest side (S3 → custom table) is owned separately by Noa in `wonderful-global`.

---

## 1. Context & goal

Today the DT-CZ KB pipeline ends in `WonderfulRAGStep`, which uploads each markdown doc
to the Wonderful KB and calls `POST /kb/files/sync` — and that sync triggers the platform's
**Gemini/Vertex** document-RAG re-indexing. Running many tenants/envs on the same cron
overwhelms Vertex (the `WONDERFULRAGSTEP__SKIP` flag is a band-aid for exactly this).

In parallel, DT-CZ has a **new, live** retrieval path (`wonderful-global`, post-#526): the KB
is sectioned into per-category Wonderful **custom tables** (`kb_<category>`) with an
auto-embedded `body` **vector** column (OpenAI `text-embedding-3-small`), queried via
`semanticSearch` from `tools/kb-search.ts`. **Vertex is not in this path at all.** It is
populated today by `scripts/kb_ingest.py`, run by hand against a local JSON export.

**Goal of this work:** add a "dumb" wurzel sink that writes the scraped KB as **one raw JSON
file** to an S3 bucket, so the new custom-table ingest can consume it — and **delete
`WonderfulRAGStep`** (the legacy Wonderful-KB/Vertex sink). S3 becomes the sole sink; the
Vertex re-index path is removed.

> **Note on the deployed legacy path:** `WonderfulRAGStep` was never committed to this repo's
> `main` (purely local work), so deleting it here is clean. But whatever currently updates the
> Wonderful KB in DT's deployment doesn't run from `main` either — removing these files does
> **not** by itself stop the live legacy/Vertex path; that deployment must be cut over separately.

**Explicitly NOT in scope here** (owned by Noa / later decisions):
- The S3 → custom-table ingestion (reuses `kb_ingest.py`, scheduled in `wonderful-global`).
- Cutting over DT's live deployment off the legacy KB path (see note above).
- Migrating tenants other than DT-CZ.

---

## 2. Architecture

```
──────── DT infra (telekom k8s) ─────────          ──────── wonderful-global (Noa) ────────
 wurzel CronJob (per tenant)
   source → … → S3MarkdownSink ──►  S3: wonderful-dtcz-kb ──► kb_ingest.py (scheduled,
                                    dt-cz/<ts>.json              single-concurrency)
                                    dt-cz/latest.json     ──►  POST custom-tables/
                                                                kb_<cat>/rows/bulk
                                                                (OpenAI auto-embed)
```

- **S3 is the sole sink.** `WonderfulRAGStep` (legacy Wonderful-KB / Vertex re-index) is deleted.
- **The S3 bucket is the contract** between this repo and Noa's ingest. wurzel writes;
  the ingest reads `latest.json`. Neither side imports the other's code.

---

## 3. The step — `S3MarkdownSink`

New package `wurzel/steps/s3/` (`__init__.py`, `settings.py`, `step.py`) — a minimal sink:
no Wonderful API, no sync, no Vertex. Just serialize and PUT.

**Type:** `TypedStep[S3MarkdownSinkSettings, list[MarkdownDataContract], list[MarkdownDataContract]]`
— a **passthrough sink**: returns its input unchanged so it can chain.

**Behaviour (`run`):**
1. If `SKIP` → log and return input unchanged (no S3 call, no creds required).
2. Serialize the **whole list to ONE JSON array** (not per-record `.md` files):
   `body = json.dumps([doc.model_dump() for doc in inpt], ensure_ascii=False)`.
   `MarkdownDataContract` is `{md, keywords, url, metadata}` (wurzel/datacontract/common.py),
   so the array is **byte-for-byte** the `[{md, keywords, url, metadata}, ...]` shape of the
   existing `KnowledgeBaseApiGather-…json` that `kb_ingest.py` already reads — no transformation,
   `metadata` preserved verbatim as a passthrough dict.
3. PUT the body to `s3://<BUCKET>/<PREFIX>/<ts>.json` where `ts` is a UTC timestamp
   (e.g. `2026-06-08T155900Z`), `ContentType: application/json`, **plus `x-amz-meta-*`
   provenance** (see below). This timestamped object is the immutable history snapshot.
4. PUT the **same body + same metadata** to `s3://<BUCKET>/<PREFIX>/latest.json` (stable pointer
   the ingest reads). Bucket versioning is enabled, so `latest.json` keeps its own history too.
5. Return `inpt` unchanged.

**Per-run provenance — S3 object metadata** (`Metadata={...}` on both PUTs; S3 prefixes them
`x-amz-meta-` and lowercases the keys, readable via `head-object` without downloading the file):

| Key | Value |
|---|---|
| `record-count` | `str(len(inpt))` |
| `run-ts` | the same `<ts>` UTC timestamp |
| `tenant` | the configured tenant (`TENANT` setting, default = `PREFIX`) |
| `source-commit` | optional — a git SHA from env if the pipeline exposes one; omit if absent |

The timestamped filename already encodes "when"; this adds count/tenant/source for cheap
queryability. All best-effort — a missing optional value is simply omitted, never a hard error.

**Failure:** raise `StepFailed` on a PUT error — there's a single object, so a failed write is
a hard failure (no partial-success semantics needed).

**Dependency:** `boto3` (or `s3fs`), added as an **optional extra** and gated behind a
`HAS_BOTO3` flag in `wurzel/steps/s3/__init__.py`, exactly like `paramiko`/`HAS_PARAMIKO`
guards `wurzel/steps/sftp/`. Keeps the core install lean.

---

## 4. Settings (`S3MARKDOWNSINKSTEP__` env prefix)

Follows the existing `WONDERFULRAGSTEP__` / `SFTPMANUALMARKDOWNSTEP__` convention.

| Setting | Required | Default | Notes |
|---|---|---|---|
| `SKIP` | no | `false` | `true` → no-op passthrough; no creds needed (per-env toggle / experiment switch) |
| `BUCKET` | yes (unless SKIP) | — | `wonderful-dtcz-kb` |
| `PREFIX` | no | `dt-cz` | key prefix; objects land at `<PREFIX>/<ts>.json` + `<PREFIX>/latest.json` |
| `TENANT` | no | = `PREFIX` | written as `x-amz-meta-tenant` provenance |
| `REGION` | no | `eu-central-1` | |
| `ENDPOINT_URL` | no | `""` | set only for MinIO / localstack tests |
| AWS creds | — | env / pod role | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` from the IAM user below, or an instance/IRSA role |

A `model_validator` requires `BUCKET` (and resolvable creds) unless `SKIP=true`, mirroring
`WonderfulRAGSettings._require_credentials_unless_skipped`.

---

## 5. Pipeline wiring

S3 is the sole sink; `WonderfulRAGStep` is deleted (`wurzel/steps/wonderful/` removed).

`local/wonderful_pipeline.py`:

```python
source  = WZ(ManualMarkdownStep)   # or the real scraperapi/docling source
s3_sink = WZ(S3MarkdownSink)        # NEW — the only sink
source >> s3_sink
pipeline = s3_sink
```

- **Single-terminal DAG** (DVC/Argo backends recurse upstream from one `pipeline` node over
  `required_steps`) — `pipeline = s3_sink` is the correct shape.
- **Deletion checklist:** remove `wurzel/steps/wonderful/` (the 3 staged-but-uncommitted files),
  and scrub any `WonderfulRAGStep` references in the gitignored `local/` (`wonderful_pipeline.py`,
  `README.md`, the `WONDERFULRAGSTEP__*` lines in the `*.env` files).

---

## 6. The S3 contract (provisioned 2026-06-08)

Bucket **`s3://wonderful-dtcz-kb`** — Wonderful AWS account `760661275542`, `eu-central-1`.
Versioning **enabled**, default encryption **AES256**, public access **fully blocked**.

```
s3://wonderful-dtcz-kb/dt-cz/<UTC-timestamp>.json   # immutable per-run snapshot
s3://wonderful-dtcz-kb/dt-cz/latest.json            # stable pointer Noa's ingest reads
```
Object body: JSON array of `[{ md, keywords, url, metadata }]`.

**IAM (scoped, this bucket only):** user `wurzel-dtcz-kb-writer`, inline policy
`wonderful-dtcz-kb-rw` granting `s3:ListBucket` + `s3:PutObject`/`s3:GetObject`. **No
`s3:DeleteObject`** — the export is append-only. Access key + the full contract live in the
gitignored `local/s3-sink.env`.

---

## 7. Testing

- Unit-test serialization (`MarkdownDataContract` list → expected single JSON array, `metadata`
  preserved) using `moto` (mocked S3) or a stubbed boto3 client — matching the repo's existing
  test style under `tests/`.
- Both objects written: assert `<ts>.json` **and** `latest.json` exist with identical bodies.
- Provenance: assert `x-amz-meta-record-count` / `run-ts` / `tenant` are set on both objects.
- `SKIP=true` → asserts no S3 client is constructed and input passes through unchanged.
- A PUT error → asserts `StepFailed`.
- Optional integration check against MinIO via `ENDPOINT_URL` (manual / CI-gated).

---

## 8. Open dependencies (not blocking the step build)

1. **Cross-account write auth** in DT's K8s deployment: the pod must carry the
   `wurzel-dtcz-kb-writer` access key (via the chart's existing env/secret mechanism), since
   wurzel runs outside account 760661275542. Static key for now; IRSA/role-assumption later if DT supports it.
2. **Bucket name/prefix sign-off with Noa** — her `kb_ingest.py --source` must point at
   `s3://wonderful-dtcz-kb/dt-cz/latest.json`.
3. **Helm/values wiring** — add the `S3MARKDOWNSINKSTEP__*` config + AWS-cred secret to the
   DT cronjob's configmap/secret (out of scope for the step PR; a deploy follow-up).
