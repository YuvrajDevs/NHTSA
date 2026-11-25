# Build log: how this was built, every decision and every failure

Written in the order things happened. Failures are numbered F1, F2... and each one has: what I saw, why it happened,
the fix, and the lesson.

---

## Phase 0. What to build and why

**Goal:** a batch data product that covers what a Data Engineer 1 role asks for and a streaming project doesn't:
SQL-heavy dimensional modelling, ETL/ELT with incremental loads, data quality and contracts, a catalog with
lineage and owners, orchestration, and data prepared for AI.

**Data:** NHTSA (US road-safety regulator) public files: consumer complaints since 1995, recalls since 1967,
defect investigations since 1972. Free, no login, updated daily, and messy in realistic ways.

**The business question the data product answers:** *"Which components are trending toward a recall, and could
complaints have warned us before past recalls?"*

---

## Phase 1. Getting the data

The files live at `static.nhtsa.gov/odi/ffdd/...` as zips of tab-separated text, with field definitions in
`CMPL.txt`, `RCL.txt`, `INV.txt`.

### F1. Downloads blocked by the network allowlist
- **Saw:** `curl: (56) CONNECT tunnel failed, response 403` for every NHTSA host.
- **Why:** the build environment only allows approved hosts (the same thing happens behind a corporate proxy).
- **Fix:** added `static.nhtsa.gov`, `vpic.nhtsa.dot.gov`, `api.nhtsa.gov`, `www.nhtsa.gov` to the allowlist.
- **Lesson:** check network access to every source on day one; in a company this is a ticket that can take a week.

### F2. Recall file returned 404
- **Saw:** `rcl/FLAT_RCL.zip 404` while complaints and investigations downloaded fine.
- **Why:** NHTSA split recalls into `FLAT_RCL_PRE_2010.zip` and `FLAT_RCL_POST_2010.zip`. Old tutorials still use the old name.
- **Fix:** the source registry (`vsi/sources.py`) lists several files per dataset; the downloader raises a clear
  "may have been renamed" error on 404 instead of a generic one.
- **Lesson:** sources change without telling you. Fail loudly and specifically.

**Download design (`vsi/download.py`):**

| Choice | Why |
|---|---|
| HEAD request first, skip if the ETag is unchanged | NHTSA republishes daily; unchanged files are reused, not downloaded again (377 MB saved on a re-run) |
| Download to `.part`, rename when complete | A crash never leaves half a zip that looks complete |
| SHA-256 + `_manifest.json` per load date | You can always prove which bytes a table came from |
| Retries with exponential backoff on network errors and 5xx | Transient failures shouldn't need a human |
| `data/landing/<load_date>/` never modified | Raw evidence; everything else can be rebuilt from it |

---

## Phase 2. Profiling before designing

I profiled every file with DuckDB before writing pipeline code (5 seconds for 2.27M rows).

| Finding | Number | Design consequence |
|---|---|---|
| Complaint rows (complaint x component) | 2,270,295 | Big enough that Spark makes sense |
| Distinct complaints (ODINO) | 1,635,218 | Grain decision: one row per complaint x component; "complaints" metric = COUNT(DISTINCT) |
| Max components in one complaint | 65 | Never `COUNT(*)` for complaints |
| Model year 9999 (unknown) | 82,693 | NULL + unknown member in dim_vehicle |
| Failure date "0" | 110,144 | NULL, measured in the quality report |
| Narratives with phone numbers / emails | 13,173 / 1,158 (first estimate) | AI layer must scrub personal data |
| Distinct state codes | 81 | Validate against a reference list of 56 |
| Complaint fields | 51 (fields 50-51 added April 2026) | Accept older 49-field layout (schema drift) |
| Recall rows / campaigns | 330,395 / 30,491 | Units affected repeat per row: campaign-level table |
| Recall text with HTML tags | 581 rows | Strip HTML in silver |
| Investigations | 154,505 rows, 5,361 investigations | One investigation spans many vehicles and campaigns |
| Components | `FUEL SYSTEM, GASOLINE:DELIVERY:FUEL PUMP` | A hierarchy -> system / subsystem levels in dim_component |

### F3. Mixed line endings and HTML inside recall text
- **Saw:** `file` reported the post-2010 recall file as "HTML document ... with CRLF line terminators".
- **Fix:** bronze strips a trailing `\r`; silver removes HTML tags and entities (`strip_html`). A contract check
  (`no HTML left in summaries`, severity error) makes sure it stays fixed.

---

## Phase 3. Bronze

Every line of every file, exactly as received, plus `_source_file`, `_load_date`, `_ingested_at`, `_field_count`
and a line hash. Partitioned by load date; re-running a date replaces that partition (`replaceWhere`), so it is
idempotent. 2.75M lines in 57 s; 1.6 GB of text -> 775 MB of Delta.

**Why keep raw lines instead of parsing straight away?** Because parsing rules will be wrong at least once (they were:
F4, F5) and NHTSA only serves today's file. With bronze, a fix is a replay, not a lost day.

---

## Phase 4. Silver: typed, cleaned, incremental

**Cleaning rules** (`vsi/transforms.py`) are Spark expressions, not Python UDFs (UDFs are 10-100x slower):
trim and collapse whitespace, upper-case names, `YYYYMMDD` -> date with invalid/placeholder dates -> NULL, `9999` -> NULL
year, `0` -> NULL for counts that NHTSA fills with zero when unknown (their 2021 change), Y/N -> boolean, component
path normalised (`STEERING: WHEEL` and `STEERING:WHEEL` become one), state validated.

**Quarantine:** empty lines, too many/too few fields, missing key, duplicate key -> `silver._quarantine` with a reason
and the raw line. Nothing is silently dropped.

**Change detection (ADR-003):** NHTSA publishes full snapshots. Each load compares the snapshot with silver on the
business key plus a SHA-256 of all columns -> I/U/D, logs the changes, MERGEs them (soft deletes), and keeps SCD2
history for recalls. A load log makes re-runs no-ops and refuses out-of-order dates.

### F4. 102,585 investigations quarantined as duplicates
- **Saw:** `silver.investigations: rows_in_snapshot=51,920, quarantined=102,585`.
- **Why:** I assumed one row per investigation x vehicle x component. Wrong: EA15001 (Takata air bags) has **194 rows**
  for the same vehicle and component, one per recall campaign it caused. After adding the campaign, 201 groups still
  collided: they differ by manufacturer (some investigations list several).
- **Fix:** key = action, make, model, year, component, **campaign, manufacturer**. Added `python -m vsi.cli rebuild
  investigations`, which replays silver from bronze. Result: 154,505 rows, 0 quarantined.
- **Lesson:** the grain is a fact about the data, not an assumption. Profile it (`GROUP BY key HAVING count > 1`).

### F5. 35,423 investigations silently lost (NULL in the key)
- **Saw:** on day 2 (no changes to investigations), 35,423 rows were reported as **new**.
- **Diagnosis:** silver held 119,082 rows, not 154,505. The missing rows all had an unknown model year (NULL). The step
  that selects rows to insert used `snapshot.join(changes, key, "left_semi")`, which compares with `=`, and in SQL
  `NULL = NULL` is not true. Those rows were dropped on day 1, "discovered" again on day 2, and dropped again.
- **Fix:** a NULL-safe semi join (`eqNullSafe` / `<=>`) everywhere a key is matched; a guard that fails the load if
  silver's live row count differs from the snapshot; a regression test (`test_null_key_parts_are_not_lost`).
- **Lesson:** any key that can contain NULL must be compared NULL-safely. And reconcile counts after every load:
  this bug produced no error at all.

---

## Phase 5. Proving change detection is exact

NHTSA only gives today's snapshot, so `scripts/simulate_next_day.py` derives a realistic day 2 with **known**
changes (deterministic seed): 250 new, 250 edited, 250 removed complaints and recalls, plus 2 broken lines.
`verify-changes` compares detected changes with the expectations key by key:

```
complaints I: expected  250, detected  250, missing 0, extra 0  PASS
complaints U: expected  250, detected  250, missing 0, extra 0  PASS
complaints D: expected  250, detected  250, missing 0, extra 0  PASS
complaints quarantined (excluding blank lines): expected 2, got 2  PASS
recalls    I/U/D: 250 / 250 / 250, missing 0, extra 0               PASS
recalls_history: exactly one current version per record             PASS
RESULT: PASS
```

---

## Phase 6. Gold: star schema and data products in SQL

12 SQL files in `sql/gold/`, run in order; each is one SELECT (ADR-002). The full gold layer (3.4M rows) builds in
~90 s. Grain decisions are in `docs/data-model.md`; the two that matter most:

1. `fact_complaint` is complaint x component, so "complaints" is `COUNT(DISTINCT complaint_id)` (metrics.yml).
2. **Units potentially affected only live in `dim_recall_campaign`.** The raw file repeats a campaign's units on every
   vehicle/component row; summing raw rows would multiply them.

**Data products:**
- `mart_component_risk_monthly`: complaints per make x component system x month on a **dense** month grid (zero months
  included, otherwise the 12-month baseline ignores quiet months and hides spikes), with a documented spike rule.
- `mart_recall_early_warning`: for every vehicle recall since 2000, complaints about the same make/model/year/system in
  the 3 years before, the first one, and whether the spike rule fired first.
- `mart_manufacturer_scorecard`: complaints, recalls, units and investigations per manufacturer group and year,
  each measure at its correct grain.

**What the data says** (19,160 vehicle recalls, 2000+):

| Recall size (units) | Recalls | Preceded by matching complaints | Spike rule fired first | Median lead of the spike |
|---|---|---|---|---|
| 100,000+ | 1,366 | 91% | 27% | ~1 year |
| 10,000-100,000 | 2,354 | 55% | 16% | ~1 year |
| 1,000-10,000 | 4,027 | 23% | 7% | 442 days |
| under 1,000 | 11,418 | 8% | 4% | 412 days |

Large recalls are almost always preceded by complaints; the simple spike rule catches about a quarter of them, a
median of a year ahead. Recalls pushed by NHTSA (ODI) had prior complaints more often (39%) than manufacturer-initiated
ones (20%), consistent with NHTSA using complaints to open investigations. Improving the signal is Milestone T1.

**Semantic layer:** `metrics.yml` defines each business metric once (table, SQL, description, owner).

---

## Phase 7. Data contracts and the quality gate

One YAML per table (ADR-006): schema, primary key, row counts, freshness, nullability, accepted values, ranges, regex,
foreign keys and custom SQL checks, each with severity. All column rules for a table run in one aggregation pass;
89 checks over ~4M rows take about a minute.

### F6. The first contract run blocked the pipeline: 6 real problems in the source
| Finding | Decision |
|---|---|
| Component name with `;` (`SERVICE BRAKES, HYDRAULIC; AUTOHOLD...`) | Data is valid, my regex was too strict: **contract changed** |
| Complaint types `OTHE` (45) and `ND` (1) not in NHTSA's own docs | Accepted, documented as undocumented codes |
| 149 complaints without a component (mostly 2003-04) | Source gap: allowed up to 0.01% (warn); unknown member in gold |
| 1,678 recall rows with ids like `21V00M000` (letters) | All 2021: pattern widened and documented |
| 19 investigations closed before they opened (1978-2000) | Source error: warn; `days_open` NULL in gold |
| 1,677 campaign ids flagged at 0.508% vs a 0.5% limit | Same `21V00x000` ids as above |

- **Lesson:** contracts are negotiated with reality. Every relaxation is written in the contract with the reason,
  so the next person knows it's deliberate.

---

## Phase 8. AI-ready documents

`gold.complaint_documents`: one row per complaint (1,625,684), narrative with personal details replaced by
placeholders, vehicle, all component systems, dates, crash/fire/injury flags, approximate tokens (median ~98, so no
chunking is needed) and a content hash. Then a TF-IDF index over the 793,316 documents since 2015 (150k terms, built
in ~110 s): `python -m vsi.cli search "brake pedal went to the floor"` returns near-identical complaints across
makes in about a second.

### F7. Twice as many phone numbers as the profile said
- **Saw:** 32,448 phone numbers scrubbed vs ~13,000 in the profiling estimate.
- **Why:** the profiling regex only matched `123-456-7890`; most are written `(913) 871-9483`, usually dealer numbers.
- **Trade-off found by sampling:** a 10-digit campaign number ("Campaign No. 2024010012") is also masked. Accepted:
  over-scrubbing loses a little text; under-scrubbing leaks personal data.
- **Lesson:** sample the matches of any PII rule before trusting its count.

| Removed | Count | Documents affected |
|---|---|---|
| Phone numbers | 32,448 | 29,690 |
| Links | 14,012 | 10,728 |
| Emails | 830 | 742 |
| VINs | 261 | 240 |

---

## Phase 9. Catalog and lineage

`python -m vsi.cli catalog` generates `docs/catalog.md` and `site/catalog.json` from the contracts (descriptions,
owners, grain, PII tags), the Delta logs (schema, rows, version, last update), the last quality run, `metrics.yml`,
and lineage **parsed from the SQL files** (FROM/JOIN). Nothing is hand-written, so it can't go stale.

---

## Phase 10. Orchestration with Airflow

`dags/vsi_daily.py`: download -> bronze (3) -> silver (3) -> gold -> quality gate -> [AI documents, catalog]; logical
date `{{ ds }}`; retries with backoff; one-slot `spark` pool; `max_active_runs=1` because snapshots must be applied
in order. `dags/vsi_rebuild.py` replays a source from bronze on demand.

### F8. Airflow image: Java wouldn't start
- **Saw:** `JAVA_GATEWAY_EXITED`, then `GLIBC_2.38 not found`, then `Module jdk.incubator.vector not found`, then
  `Error loading java.security file`.
- **Why:** PySpark needs a full Java runtime in the Airflow image. A JDK copied from a newer Linux needs a newer C
  library; a minimal PyPI Java lacks the module Spark 4 loads; Java's security config lives in `/etc`, outside the JDK folder.
- **Fix:** the Dockerfile installs `openjdk-17-jre-headless` from the distribution (works on a normal machine). In the
  locked-down build sandbox I used Ubuntu 22.04's package (older C library) including its `/etc` files.
- **Lesson:** a binary depends on more than its own folder: C library version, modules, config files.

### F9. A broken task looked like a hung pipeline
- **Saw:** the first DAG test ran for 30+ minutes without progress.
- **Why:** each task failed in seconds, then waited for retries with exponential backoff (5, 10 min...).
- **Fix:** retries come from `VSI_TASK_RETRIES` (2 in production, 0 in development).
- **Lesson:** retries are for transient errors; for deterministic errors they only delay the bad news.

### F10. The quality gate stopped the sample run (correct behaviour), then the AI task found nothing to index
- **Saw 1:** with `VSI_SAMPLE_ROWS=20000`, the gate failed on "row count >= 2,000,000"; the AI and catalog tasks never ran.
  That is the gate doing its job. Sample mode now reports row-count minimums as warnings.
- **Saw 2:** `ValueError: empty vocabulary`: the first 20,000 complaints are all from the 1990s, and the index only
  covered 2015+. Now falls back to all documents when the recent set is tiny.
- **Result:** all 11 tasks green in `airflow dags test vsi_daily 2026-11-25` inside Docker.

---

## Phase 11. Performance: the daily MERGE

A daily update changes ~750 of 2.3M complaint rows, yet the first version wrote **1.79 GB**: a Delta MERGE rewrites
every data file holding a matched row, changes are scattered over all 16 files, and I ran two MERGEs (upserts, then
soft deletes). Merging both into **one MERGE** with an `_op` flag halved it: 0.89 GB written, MERGE time 70 s -> 44 s
(-38%), numbers from Delta's transaction log. Full write-up: `docs/performance.md`.

### F11. Deletion vectors: out of memory while planning the MERGE
- **Saw:** `java.lang.OutOfMemoryError: Java heap space` on the day-2 MERGE with `delta.enableDeletionVectors = true`,
  with a 6 GB and then a 10 GB driver. The stack was in Catalyst (`transformDownWithPruning`): the memory went on
  **planning** the query, not on data.
- **Decision:** deletion vectors off by default (setting kept for later experiments); fixed the bigger waste
  (two MERGEs) instead. Two hours of Delta internals would not have been a good trade for this project.
- **Lesson:** a feature that should help can fail for reasons unrelated to data size. Measure, and keep a fallback.

### F12. An experiment script that hid its own error
- **Saw:** the experiment failed with only "returned non-zero exit status 1".
- **Why:** my script sent the child process output to `/dev/null`.
- **Fix:** capture output and print the tail on failure.
- **Lesson:** never discard stderr in automation; it's the only clue you get.

---

## Phase 12. Tests and CI

15 tests (`pytest`): cleaning rules on tiny DataFrames; silver end to end over two days (I/U/D, quarantine, soft
delete, idempotent re-run, out-of-order refusal); the NULL-key regression (F5); the contract engine catching each kind
of violation; PII scrubbing; gold on a fixture (fact reconciles with silver, units counted once per campaign, early
warning lead time). GitHub Actions runs them with Java 17 on every push (~5 min).
