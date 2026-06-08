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

**Goal of this work:** add a "dumb" wurzel sink that writes the scraped KB as raw JSON to an
S3 bucket, so the new custom-table ingest can consume it — running **alongside** the legacy
`WonderfulRAGStep`, not replacing it. This enables a true A/B experiment: same scraped source,
two indexes (legacy Vertex KB vs. new custom-table vector RAG), compared on retrieval quality.

**Explicitly NOT in scope here** (owned by Noa / later decisions):
- The S3 → custom-table ingestion (reuses `kb_ingest.py`, scheduled in `wonderful-global`).
- Retiring `WonderfulRAGStep` / removing Vertex — that's a future cutover once the experiment
  picks a winner.
- Migrating tenants other than DT-CZ.

---

## 2. Architecture

```
──────── DT infra (telekom k8s) ─────────          ──────── wonderful-global (Noa) ────────
 wurzel CronJob (per tenant)
   source → … → S3MarkdownSink ──┐
                                 ▼
   WonderfulRAGStep ──► Wonderful KB     S3: wonderful-dtcz-kb ──► kb_ingest.py (scheduled,
       (legacy, Vertex re-index)         dt-cz/<ts>.json              single-concurrency)
                                         dt-cz/latest.json     ──►  POST custom-tables/
                                                                     kb_<cat>/rows/bulk
                                                                     (OpenAI auto-embed)
```

- **The S3 bucket is the contract** between this repo and Noa's ingest. wurzel writes;
  the ingest reads `latest.json`. Neither side imports the other's code.
- **Additive only.** Nothing existing is removed; today's pipeline keeps working untouched.

---

## 3. The step — `S3MarkdownSink`

New package `wurzel/steps/s3/` (`__init__.py`, `settings.py`, `step.py`), structurally
mirroring `wurzel/steps/wonderful/` but much simpler — no Wonderful API, no sync, no Vertex.

**Type:** `TypedStep[S3MarkdownSinkSettings, list[MarkdownDataContract], list[MarkdownDataContract]]`
— a **passthrough sink**: returns its input unchanged so it can chain (same pattern as
`WonderfulRAGStep`).

**Behaviour (`run`):**
1. If `SKIP` → log and return input unchanged (no S3 call, no creds required). Mirrors
   `WonderfulRAGStep`'s no-op mode for per-env toggling.
2. Serialize: `payload = [doc.model_dump() for doc in inpt]` → `json.dumps(payload, ensure_ascii=False)`.
   `MarkdownDataContract` is `{md, keywords, url, metadata}` (wurzel/datacontract/common.py),
   which is **exactly** the `[{md, keywords, url, metadata}]` shape `kb_ingest.py` already reads —
   no transformation.
3. PUT the body to `s3://<BUCKET>/<PREFIX>/<ts>.json` where `ts` is a UTC ISO-ish timestamp
   (e.g. `2026-06-08T155900Z`), `ContentType: application/json`.
4. PUT the **same body** to `s3://<BUCKET>/<PREFIX>/latest.json` (stable pointer the ingest reads).
   Bucket versioning is enabled, so `latest.json` keeps its own history too.
5. Return `inpt` unchanged.

**Failure:** raise `StepFailed` on a PUT error (unlike `WonderfulRAGStep`'s per-doc tolerance —
there's a single object, so a failed write is a hard failure).

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
| `REGION` | no | `eu-central-1` | |
| `ENDPOINT_URL` | no | `""` | set only for MinIO / localstack tests |
| AWS creds | — | env / pod role | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` from the IAM user below, or an instance/IRSA role |

A `model_validator` requires `BUCKET` (and resolvable creds) unless `SKIP=true`, mirroring
`WonderfulRAGSettings._require_credentials_unless_skipped`.

---

## 5. Pipeline wiring

`local/wonderful_pipeline.py`:

```python
source         = WZ(ManualMarkdownStep)   # or the real scraperapi/docling source
s3_sink        = WZ(S3MarkdownSink)        # NEW
wonderful_sink = WZ(WonderfulRAGStep)      # KEPT (legacy Vertex path)
source >> s3_sink >> wonderful_sink        # S3 export runs FIRST
pipeline = wonderful_sink
```

- **S3 before Wonderful**, because `WonderfulRAGStep` raises `StepFailed` when all docs fail
  (a KB/Vertex hiccup). Running the S3 export upstream means Noa's data is written **before**
  any legacy-KB call — a Vertex problem can never block the export the experiment depends on.
  Both steps are passthrough, so `wonderful_sink` still receives the identical doc list.
- **The DAG is single-terminal** (DVC/Argo backends recurse upstream from one `pipeline`
  node over `required_steps`), so two independent sink leaves aren't expressible — chaining
  the two passthrough sinks is the correct shape, not fan-out.
- **Experiment toggle, no code change:** flip each path via its `__SKIP` env —
  both on (dual-write A/B) · `WONDERFULRAGSTEP__SKIP=true` (S3-only, no Vertex) ·
  `S3MARKDOWNSINKSTEP__SKIP=true` (legacy only, today's behaviour).

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

- Unit-test serialization (`MarkdownDataContract` list → expected JSON array) and the
  `latest.json` second-PUT, using `moto` (mocked S3) or a stubbed boto3 client — matching the
  repo's existing test style under `tests/`.
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
