# Danish Democracy

A full-history Trilogy voting model for [Folketinget's open parliamentary data](https://www.ft.dk/dokumenter/aabne_data), inspired by [bgarcevic/danish-democracy-data](https://github.com/bgarcevic/danish-democracy-data). DuckDB reads seven compact Parquet tiles; no warehouse account is required for queries once the tiles are published.

**Status: model and extraction implementation only. No public tiles have been published and no Cloud schedule is active.** The proposed Cloud declaration is an opt-in template, deliberately excluded from automatic deployment. The `.unpublished` marker keeps it out of the Studio catalog. Merging this change alone does not activate refresh or advertise unavailable public data. Production release needs an initial backfill and a practical, verified deletion-reconciliation route.

## Coverage and semantics

| File | Grain | Contents |
|---|---|---|
| `ballot.preql` | individual ballot ID (`Stemme.id`) | Actor, roll call and ballot-type relationships |
| `vote.preql` | roll-call ID (`Afstemning.id`) | Official approval/result, correction comments, meeting and optional case step |
| `actor.preql` | actor ID | Public actor name, actor type, group abbreviation, validity dates |
| `meeting.preql` | meeting ID | Date, title, room, type/status and parliamentary period |
| `period.preql` | period ID | Period code, name, type and dates |
| `case.preql` | case ID | Title, number, type/category/status and decision metadata |
| `case_step.preql` | case-step ID | Step type/status/date and case relationship |

All available upstream history is intended; no arbitrary 2014 cutoff or rolling window is applied. Parliamentary proceedings generally start in October 2013, while dimensions may contain older records. These are the analytical equivalents of the upstream project’s voting fact and dimensions, with native IDs rather than hashed surrogate keys. Case and case-step grain remain separate. Unused relationship tables and the upstream holiday calendar are not reproduced.

Important distinctions:

- Count roll calls from `vote.id`; joined ballot rows are a different population
- Ballot types are **For**, **Imod**, **Fravær** and **Hverken for eller imod**. Absence is not abstention
- Official `approved`, `conclusion` and `comment` are retained; raw ballot totals can differ from the official result because corrections may be recorded in comments
- Optional/missing dimension relations preserve the underlying fact. A roll call with no case step is still a valid roll call
- Current-party biography fields are deliberately absent; assigning today’s party to historical ballots would be misleading
- Biographies, birth/death dates, private contact details, document bodies, photographs and document downloads are outside this extract
- Source `opdateringsdato` is preserved verbatim as `source_updated_at` (including seven fractional digits) for replay; `updated_at` is its microsecond-resolution analytical timestamp. Upstream naive timestamps are not silently asserted to be UTC

Examples are in `examples/duckdb/danish_democracy/`. For instance:

```trilogy
import vote;
SELECT approved, count(id) -> roll_calls
ORDER BY roll_calls desc;
```

## Refresh design and the unresolved production gate

The existing repository workflow discovers `[cloud]` in `trilogy.toml` only. `data/trilogy_cloud.example.toml` proposes `operation = "refresh"` and `0 0 5 * * *` (six-field cron, daily **05:00 UTC**). It uses the existing `trilogy-data` organization, GCS bucket and `GOOGLE_HMAC_KEY` / `GOOGLE_HMAC_SECRET` secret names. It does not create credentials or new infrastructure. UTC is intentional; the local Copenhagen hour changes with daylight saving time.

Daily refresh is appropriate for parliamentary updates. The official SQL Server backup is generated overnight and is the preferred candidate for a sustainable full-snapshot production pipeline. The public API currently caps responses at 100 rows. On 2026-10-12 it reported approximately 1.84 million ballots, 224,803 case steps, 99,629 cases, 18,419 actors, 13,981 meetings, 10,535 roll calls and 163 periods: about **22,100 pages per complete pass**, before retries and lookup calls. Measured source requests took several seconds each; full backfill/reconciliation can take many hours. An initial backfill plus a second ID pass can exceed 44,000 requests. No full backfill was run during implementation.

The API documentation describes `Slettet` and `SlettetMap`, but neither is exposed in the current service metadata and `/api/Slettet` returns 404. Consequently the fallback extractor **never assumes that a merge by update timestamp processes deletions**:

1. Fetch allowlisted columns, upserting each entity independently by ID with an inclusive, lossless update cursor
2. Scan authoritative live IDs at least every seven days, and immediately on a post-upsert count mismatch; `ids_reconciled_at` records the last complete scan
3. Remove IDs no longer present, and verify counts plus the latest update timestamp stayed stable before/after the extraction
4. Produce output only after those checks pass. Incomplete paging, request-budget exhaustion, drift or missing IDs fails without replacing the existing output file

This is conservative eventual reconciliation, not an atomic snapshot guarantee from a transactional API. The seven published tiles would also be replaced independently, not as one cross-table transaction. Deletions that a concurrent API change masks from the count check may remain until periodic reconciliation. A complete production design must address that eventual-consistency window and deleted identifiers in retained object versions/caches and downstream copies. No bucket retention, ACL, lifecycle, cache or credential settings are changed here.

The API fallback is therefore **not enabled as an unattended daily production job**. Its default 1,000-request budget intentionally prevents accidental unbounded ingestion; do not simply enable the template and assume a full history refresh fits its one-hour timeout. Before activation, choose and verify one of:

- Restore the official nightly SQL Server backup in an authorized existing environment, extract the same allowlisted schema, validate all tiles, and publish/reconcile removed data
- Establish a supported tombstone feed plus an appropriately bounded periodic full-ID reconciliation
- Use the resumable API extractor on an existing persistent worker with durable SQLite checkpoints for all seven entities, a measured request budget, and enough runtime for both the full backfill and periodic full-ID scans

A scoped deployment token may also be unable to assign `secret_env` to a new job; if so, an existing authorized Cloud operator must bootstrap that binding. No login/token was available in the implementation environment, so Cloud API state, bucket access, server resource limits and live schedule behavior were not validated.

### Runtime investigation (2026-10-13)

The republished executor can reach GitHub, Library downloads and the official API. The archive was restored at base `0fb759bafc47a04157effd4c4d5b2284b66af32f`. Validation uses Python 3.13 and pytrilogy 0.3.383, above the Cloud workflow's minimum 0.3.317. `trilogy cloud whoami` reports **Not logged in**; no Cloud token, HMAC key/secret or Google application credentials are configured in the executor. No login or credential creation was attempted.

Repository configuration confirms that the existing COVID ingest uses bucket `trilogy_public_models` and the two HMAC secret names above. That is evidence of the intended binding, not verification of the live secrets or their permissions. GitHub's read-only environment endpoint confirms that `refresh` has a custom deployment branch policy; its secret-metadata endpoint returns HTTP 403 `Resource not accessible by integration`. The workflow documents and enforces the main-only deployment route. This investigation did not dispatch it or attempt to bypass that denial.

The remaining full-history route is:

1. An existing authorized operator identifies a persistent runtime and verifies existing bucket/Cloud bindings. The API path needs writable durable checkpoint storage and a single writer per entity; this executor's files are not evidence of durable production storage. The SQL Server backup alternative still needs an existing authenticated restore runtime; none is established here.
2. Run the manual extractor for all seven entities, retaining each SQLite checkpoint across bounded invocations. Use the same checkpoints for daily incremental updates and at least weekly full-ID reconciliation. The current ephemeral Cloud `emit` path restores from published tiles and cannot preserve an interrupted large reconciliation, so it is not a substitute for that worker. Measure elapsed time, peak memory, disk and request totals at ballot scale before selecting limits.
3. Validate a complete generation, including all examples, keys, source counts, nullable relationships and deletion propagation. The offline preparation command below stages immutable generation paths and a pinned query model. A production publisher must still upload/verify every object, then conditionally switch one manifest/pointer. Consumers must resolve that pointer once and pin the generation's model for all seven tables. **Remote publication and pointer resolution are not implemented by the fixed-URL default models in this patch**; replacing seven fixed URLs sequentially does not meet that requirement. Retention/cache removal policy must also be agreed before release.
4. With separate release/activation approval, wire the verified runtime and publication mechanism, then verify the exact deployed source version, a terminal successful refresh, public queries and a subsequent incremental/reconciliation run. Only then activate the daily 05:00 UTC schedule and remove `.unpublished`.

Full history plus incremental updates is the selected scope. No smaller-window fallback, production merge, public upload or schedule activation is authorized by this preparation step.

## Resumable API backfill (manual fallback)

The loader uses bounded requests, retries transient errors, honors numeric Retry-After, projects columns at the API, and checkpoints progress locally. It reads no linked documents or biographies. Each invocation can stop with exit code 3 and be rerun with the same checkpoint. Both the full-row pass and ID pass resume while the source remains stable. Do not publish an intermediate checkpoint or an unfinished output.

```bash
MODEL=trilogy_public_models/duckdb/danish_democracy
uv run "$MODEL/data/ingest/source.py" ballot \
  --checkpoint-dir "$MODEL/.checkpoints" \
  --output "$MODEL/ballot.parquet" \
  --max-requests 1000
```

Repeat per entity (`actor`, `period`, `meeting`, `case`, `case_step`, `vote`, `ballot`). Exit 0 means that entity completed reconciliation and its output was atomically replaced. Exit 3 means no new output was published. A changing source may require retries. The API does not promise transactional isolation, so coordinate the initial release at a quiet time and validate source counts immediately before publication.

Once all seven complete local files exist, an operator with existing bucket access can use the native Trilogy ingest project to publish them:

```bash
# Review targets first. This performs no upload by itself.
trilogy refresh "$MODEL/data" --dry-run

# This command writes public GCS tiles. Run only after release approval,
# with existing authorized HMAC credentials and freshly validated seeds.
DANISH_ODA_BACKFILL_DIR="$(pwd)/$MODEL" trilogy refresh "$MODEL/data"
```

Do not use branch-environment rehearsals with the current API loader: it reads production seed URLs, while Trilogy may namespace write targets. The opt-in template must remain disabled until the production/rehearsal seed strategy and reconciliation runtime have been verified.

## Offline immutable-generation preparation

Stop extraction writers after all seven outputs complete and pass source-level validation, then run:

```bash
uv run "$MODEL/data/ingest/generation.py" \
  --source-dir "$MODEL" --output-dir /path/to/local/generations
```

This command copies all seven Parquet files into a private staging directory, validates exact schemas, required values, unique IDs and reconciliation timestamps, and records row counts, sizes and SHA-256 hashes. It hashes data plus model source to name a generation, writes eight query-model files pinned to that generation's immutable public paths, and exposes the completed directory with a local rename. Failed preparation exposes no partial generation; an existing generation is never overwritten. It performs no network calls, uploads, pointer updates or activation. All four examples are tested against a locally resolved generation model.

The resulting directory is the proposed object tree below `duckdb/danish_democracy/generations/<generation>/`, including `manifest.json` and `model/entrypoint.preql`. A future authorized publisher must create objects without overwriting existing generations, verify remote hashes, and use an object-version precondition to change one current-generation pointer only after the entire bundle is present. A reader must resolve that pointer once per session and load that generation's entire model. Existing sessions remain on their pinned generation; separate per-table pointer reads are unsafe. Pointer compare-and-swap, reader resolution, deletion propagation and retention remain runtime-specific work. Local rename guarantees visibility on the local filesystem, not remote atomicity or an atomic upstream snapshot.

Inspection of the existing FAA publishers found independent per-object uploads (`publish_dimensions.py`), not a cross-table generation switch. The installed pytrilogy 0.3.383 refresh command's state snapshots record asset observations and fingerprints; they do not retain this extractor's SQLite rows or paging checkpoints. Its Cloud workspace schema carries versioned files/configuration and resource defaults, with no verified writable-volume contract here. Those mechanisms cannot be assumed to provide durable incremental extraction state. The selected worker must explicitly retain the checkpoint directory across invocations.

## Verification and activation checklist

1. Run `pytest tests/test_danish_democracy.py tests/test_studio_manifests.py`, `ruff check trilogy_public_models`, `mypy trilogy_public_models`, and `python studio/build.py --check`
2. Complete the chosen full-history seed, validate key uniqueness, required fields, source counts and nullable relationships, and inspect all four examples against real data
3. Validate a deletion fixture through the selected production ingestion route, including removal from derived/public outputs and retained accessible versions
4. Confirm existing Cloud secret bindings and runtime/budget, then copy only the approved `[cloud]` configuration into `data/trilogy.toml` in a separate reviewed change
5. After approved merge, inspect the exact-commit **Cloud Sync** workflow result, check the job’s source version and schedule with `trilogy cloud --org trilogy-data jobs list` / `schedules list`
6. Trigger that verified job with `jobs run <job-id> --wait`; inspect the final logs and public Parquet row counts/freshness. A queued run or a successful sync is not proof of a successful refresh

7. Only after the verified public queries succeed, remove `.unpublished`, regenerate the Studio manifests and publish the catalog entry

Offline tests cover typed schemas, models and examples, nullable case-step joins, timestamp ties, removal plus insertion with unchanged total count, incomplete backfill, request-budget paging, source drift and prohibited-column exclusions. The cross-dataset live validation suite is not a substitute for this pre-publication gate and skips this model until its tiles exist.

Revalidation on 2026-10-13 (Python 3.13, pytrilogy 0.3.383):

- `pytest tests/test_danish_democracy.py tests/test_studio_manifests.py tests/test_basic_structure.py -q`: **20 passed**, including retry-budget and complete-generation/failure-isolation coverage
- `mypy trilogy_public_models`: **49 source files passed**
- `ruff check trilogy_public_models tests/test_danish_democracy.py`: passed. Eleven baseline UP017 findings in seven COVID/FAA files were fixed in a separate mechanical UTC-alias commit so CI can reach the tests; no ingest behavior changed
- `python studio/build.py --check`: passed; the Danish model remains absent from the catalog
- `python -m build --wheel --no-isolation`: passed; the wheel contains `data/ingest/entities.json`
- Seven live allowlisted entity samples converted successfully to their Arrow schemas; a live Periode backfill and incremental rerun each produced **163 rows**

These checks do not constitute ballot-scale backfill, atomic publication, authenticated Cloud/bucket validation or a first successful production refresh.

## Attribution and conditions

Source: **Folketinget (the Danish Parliament), Open Data**, https://oda.ft.dk/, accessed 2026-10-12. Processing/modeling: Trilogy public models. This is an independent model and does not imply endorsement by Folketinget.

Use is governed by [Folketinget’s conditions](https://www.ft.dk/-/media/sites/ft/pdf/dokumenter/aabne-data/conditions_for_use_-of_the_danish_parliaments-_open_data.pdf), not by the reference implementation’s software license. Retain source attribution and conditions when redistributing. Correct or remove material that Folketinget retracts; consumers must propagate removal to their copies, including mistaken disclosures. The conditions do not grant unrestricted reuse of linked photographs or documents, which are excluded here. The source is provided without warranties; derived statistics should retain the official vote outcomes and correction comments.

The architectural reference project is MIT-licensed. This model and extractor were independently implemented against the official API schema; no upstream code or biography content is copied.
