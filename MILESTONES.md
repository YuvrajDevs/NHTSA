# Milestones: understand it, run it, break it, extend it

The pipeline works end to end. To **own** it and defend it in interviews, go through these in order. Each has what
to read, what to run, and questions you must be able to answer out loud.

## Part A: understand and run (days 1-4)

### M0. Set up (30 min)
README quick start. Done when `pytest -q` passes. **Explain:** why does PySpark need Java? What is a virtual environment?

### M1. The data (read before any code)
Open `data/landing/<date>/CMPL.txt`, `RCL.txt`, `INV.txt` and build-log Phase 2.
**Explain:** what is one row of each file? Why is "complaints" `COUNT(DISTINCT odino)`? Why can't you sum
`potentially_affected` from the raw recall rows?

### M2. Bronze and silver
Read `vsi/bronze.py`, `vsi/transforms.py`, `vsi/silver.py`. Run `python -m vsi.cli bronze` and `silver` for one source.
Query `silver._quarantine` and `silver._load_log`.
**Explain:** why keep raw lines? How does change detection work with full snapshots? What happens if you re-run
the same date? An older date? What is a soft delete and why not delete?

### M3. The two real bugs
Read build-log F4 and F5. Then reproduce F5: replace `semi_join(...)` with a plain `left_semi` join, run the test
`pytest tests/test_silver.py -k null_key`, watch it fail, put it back.
**Explain without notes:** why NULL = NULL is not true, and why the bug produced no error.

### M4. Gold and the SQL
Read every file in `sql/gold/` in order and `docs/data-model.md`. Write 5 queries of your own against gold (e.g. top 10
components by crash complaints for one make, recalls per year by `influenced_by`).
**Explain:** grain of each fact; why hash surrogate keys; what an unknown member is; why the risk mart needs zero months.

### M5. Quality, catalog, AI
Run `quality`, open `docs/quality-report.md`; break a contract on purpose (set a range too tight) and watch the run fail.
Run `catalog` and read `docs/catalog.md`. Run `search` with your own descriptions.
**Explain:** error vs warning severity; why contracts generate the catalog; why AI reads a scrubbed table.

### M6. Change detection proof and Airflow
Run `scripts/simulate_next_day.py`, then bronze, silver and `verify-changes` for the next day. Start Airflow
(`docker compose up -d --build`), trigger `vsi_daily`, then `vsi_rebuild` with `{"source": "recalls"}`.
**Explain:** logical date vs today; why `max_active_runs=1`; why the quality gate has `retries=0`.

## Part B: extend it (your code, your story)

### T1. A better early-warning signal ★
Today's spike rule catches ~27% of big recalls a year ahead (make x system level). Try: model-level (make + model +
system), a rate per vehicles on the road instead of raw counts, crash/fire-weighted counts, or a rolling z-score.
Measure with `mart_recall_early_warning`: share of recalls flagged, median lead time, and how many spikes were NOT
followed by a recall (false alarms). **Done when** you can show a before/after table.

### T2. vPIC enrichment ★
Call NHTSA's public vPIC API (`vpic.nhtsa.dot.gov/api/vehicles/GetModelsForMakeYear/...`) to standardise make/model
names (`F150` vs `F-150`). Paginate, cache responses, rate-limit, retry. Add a `dim_vehicle.model_canonical` column.

### T3. Scale test
Multiply complaints x20 (~45M rows, change ids) and run silver + gold. Find the slowest stage in the Spark UI
(http://localhost:4040), fix one thing (partitioning, broadcast, skew), record before/after in `docs/performance.md`.

### T4. dbt port
Move `sql/gold/` to dbt models (dbt-spark or dbt-duckdb), turn contract checks into dbt tests, compare with this setup.

### T5. Run it on Databricks Free Edition
Same package; storage paths in a Unity Catalog volume; a Databricks Job with the same tasks as the DAG; register gold
tables in Unity Catalog with the descriptions from `contracts/`.

### T6. A dashboard
Streamlit or Power BI on `mart_component_risk_monthly` and `mart_recall_early_warning`, using only `metrics.yml` definitions.

## Interview questions this project prepares you for
1. Walk me through your pipeline. (README diagram, 2 minutes)
2. How do you load data incrementally when the source only gives full snapshots? (ADR-003, Phase 4)
3. How do you make a pipeline idempotent? What happens on a re-run? (load log, replaceWhere, ADR-008)
4. Tell me about a data quality issue you found. (F6: six real issues; F4: wrong grain)
5. Tell me about a bug that produced no error. (F5: NULL key, reconciliation guard)
6. What is the grain of your fact table? Why? (data-model.md)
7. What is SCD type 2? Where did you use it and where not, and why? (ADR-005)
8. How do you handle personal data? (contracts PII tags, scrubbed AI table, F7)
9. How would you make this data ready for AI / an LLM? (ADR-010, complaint_documents)
10. How did you make it faster? (docs/performance.md)
