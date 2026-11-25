# Project status: in progress

Last updated: 2026-11-25. The core pipeline runs end to end on the real NHTSA data. What is left is mostly one clean
final run, publishing its generated reports, and finishing the write-up. Nothing below "Left to do" is claimed anywhere.

## Done (and verified)

| Part | Evidence |
|---|---|
| Profiling of all three NHTSA files (2.27M complaint, 330K recall, 154K investigation rows) | build-log Phase 2 |
| Download with ETag skip, SHA-256 manifest, retries | `vsi/download.py`; second run skipped all 7 unchanged files |
| Bronze: every line as received, idempotent per load date | `vsi/bronze.py`; 2.75M lines in ~1 min |
| Silver: typing, cleaning, quarantine with reasons, daily change detection (I/U/D), soft deletes, SCD2 for recalls, load log (idempotent + ordered), rebuild from bronze | `vsi/silver.py`, `tests/test_silver.py` |
| Change detection proven exact on a simulated day | `verify-changes`: 750/750 complaint and 750/750 recall changes, 0 missing, 0 extra (build-log Phase 5) |
| Two real bugs found and fixed: wrong investigation grain (F4), 35,423 rows silently lost to a NULL key (F5) | build-log F4, F5; regression test |
| Gold: 12 SQL models (6 dimensions, 3 facts, 3 data-product marts) | `sql/gold/`, `docs/data-model.md`; built in ~90 s |
| Semantic layer | `metrics.yml` |
| Data contracts: 89 checks on 15 tables, blocking quality gate; first run found 6 real source issues | `contracts/`, `vsi/quality.py`, build-log F6 |
| AI-ready documents: 1.63M complaints, 47,551 personal details scrubbed, TF-IDF similarity search | `vsi/ai.py`, build-log Phase 8 and F7 |
| Generated catalog with lineage parsed from SQL | `vsi/catalog.py` |
| Airflow: daily DAG + rebuild DAG, all 11 tasks green in Docker (sample mode) | `dags/`, build-log Phase 10, F8-F10 |
| Daily MERGE tuning: 1.79 GB -> 0.89 GB written, 70 s -> 44 s | `docs/performance.md`, build-log Phase 11 |
| 15 pytest tests, GitHub Actions CI workflow | `tests/`, `.github/workflows/ci.yml` |
| Docs: README, build log (F1-F12), data model, 12 ADRs, milestones, resume kit, project page | `docs/`, `site/` |

## Left to do

1. **One clean final run on real data only** (`rm -rf lake && python -m vsi.cli run`). The current local lake also
   contains the simulated test day, and the last full run was stopped before gold finished.
2. **Commit the generated reports from that run:** `docs/quality-report.md`, `docs/catalog.md`,
   `docs/ai-readiness.json`, `site/catalog.json`, and rebuild `site/data.json` (`python scripts/build_site.py`), which
   also adds the example searches. These were left out of the repo on purpose: the copies on disk came from a
   20,000-row test run and would show the wrong numbers.
3. **Re-check every number in README and RESUME.md against that run.** The early-warning table was computed before the
   simulated day was applied and should only move by a few tenths of a percent.
4. **Confirm CI passes on GitHub** (first push; it runs Spark tests with Java 17).
5. **Turn on GitHub Pages** for the project page (Settings -> Pages -> Source: GitHub Actions).
6. **Learning-platform walkthrough** of this project, like the Turbofan one.
7. Optional extensions are in `MILESTONES.md` Part B (better early-warning signal, vPIC enrichment, scale test,
   dbt port, Databricks, dashboard).
