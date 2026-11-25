# Vehicle Safety Intelligence: resume and interview kit

> Project is **in progress** (see STATUS.md). Until it is finished, use the "in progress" wording below, not the full bullets.

**In-progress resume entry (use this now):**
- Building a governed data product on public NHTSA vehicle complaints (2.3M rows), recalls and investigations, to test whether consumer complaints could have warned of recalls.
- Built so far: a medallion pipeline that turns daily full snapshots into change data capture (inserts, updates, soft deletes, SCD type 2 for recalls), checked on a simulated day with 1,500 known changes (none missed, none extra); a star schema in SQL; and 89 data-contract checks run as an Airflow quality gate.

**Full entry (for when it is finished):**

## Resume entry (pick 3-4 bullets)

**Vehicle Safety Intelligence: governed batch data product on public NHTSA data** · PySpark, Delta Lake, SQL, Airflow,
Python · [code](https://github.com/YuvrajDevs/NHTSA)

- Built a medallion lakehouse (PySpark + Delta Lake) over **2.3M vehicle complaints, 330K recall and 154K
  investigation records**, turning NHTSA's daily full snapshots into incremental change data capture (inserts, updates,
  soft deletes, SCD type 2), verified exact on a simulated day: **1,500/1,500 changes detected, 0 missing, 0 extra**.
- Designed a **star schema in SQL** (3 facts, 6 dimensions, hash surrogate keys, unknown members) and 3 data-product
  marts; showed **91% of large recalls were preceded by matching complaints**, with a spike signal firing a median
  **~1 year** ahead.
- Enforced **data contracts** (89 checks: schema, keys, ranges, foreign keys, freshness) as a blocking Airflow quality
  gate; the first run surfaced **6 real defects in the source data**, and count reconciliation caught a silent
  NULL-key join that had **dropped 35K rows**.
- Prepared **1.6M complaint narratives for AI**: scrubbed 47K personal details (phones, emails, VINs), added metadata,
  and built a similarity search; generated a data catalog with owners, PII tags and **lineage parsed from SQL**.
- Orchestrated with **Airflow** (logical dates, retries, pools, rebuild DAG) in Docker; 15 pytest tests and CI on
  GitHub Actions; cut the daily MERGE from 1.79 GB to 0.89 GB written (-38% time) by
  combining upserts and soft deletes into one MERGE, measured from the Delta transaction log.

**Short version (one line):** Batch lakehouse data product on 2.3M NHTSA vehicle complaints and recalls (PySpark, Delta,
SQL star schema, Airflow): incremental CDC verified exact, data contracts as a quality gate, AI-ready documents.

## 30-second pitch

"I built a data product on public US vehicle-safety data: 2.3 million consumer complaints plus every recall and
defect investigation. NHTSA only publishes full files every day, so my pipeline compares each day's snapshot with
the current tables to find what was inserted, changed or removed, and keeps version history for recalls. I proved
that on a simulated day with known changes: every change detected, nothing extra. On top I modelled a star schema
in SQL and built marts that answer whether complaints could have warned us before recalls: for big recalls, 91%
were preceded by complaints. Data contracts run as a quality gate in Airflow and found six real problems in
NHTSA's data on the first run, and a reconciliation check caught a bug that had silently dropped 35,000 rows."

## How it maps to the Cummins Data Engineer 1 posting

| Posting asks for | Where this project shows it |
|---|---|
| Data products for business domains (Quality, Product Lifecycle, Supply Chain) | Vehicle quality data product: complaints -> investigations -> recalls lifecycle |
| Make data findable, understandable, trusted | Generated catalog with owners, descriptions, PII tags, lineage; contracts; metrics.yml |
| ETL/ELT, ingestion, incremental loads | Conditional download, bronze/silver/gold, snapshot CDC, SCD2, idempotent reruns |
| Data modelling for analytics | Star schema with explicit grains, surrogate keys, role-playing date, marts |
| Data quality, validation | Contracts with severities, quarantine with reasons, reconciliation guard |
| SQL | 12 gold models in plain SQL: windows, dense grids, range joins, max_by, full outer joins |
| Spark / big data | PySpark on 1.6 GB text, MERGE, deletion vectors, NULL-safe joins, partitioning |
| Orchestration / automation | Airflow DAGs with logical dates, pools, retries, gate, rebuild |
| Data ready for AI | Scrubbed, metadata-rich documents and a retrieval index |
| Working with stakeholders, documentation | Business question, findings table, build log, ADRs, project page |

## Be ready to answer (with where the answer lives)

1. Walk me through the architecture. -> README diagram
2. How do you load incrementally from full snapshots? How did you prove it? -> ADR-003, build-log Phase 5
3. What makes your pipeline idempotent? -> load log, replaceWhere, ADR-008
4. What is the grain of fact_complaint, and why does it matter? -> data-model.md
5. Why can't you sum potentially_affected from recall rows? -> dim_recall_campaign
6. What is SCD2, and why only for recalls? -> ADR-005
7. Tell me about a data quality issue you found. -> F6 (six), F4 (grain)
8. Tell me about a bug that produced no error. -> F5 (NULL key, 35K rows)
9. How do you handle PII? Make data AI-ready? -> contracts PII tags, F7, ADR-010
10. What would you change for production on Azure / Databricks? -> ADLS + Unity Catalog, Databricks Jobs or ADF,
    contracts in CI, alerting on freshness, MILESTONES T5

## Honest framing

Say "I built" for what you can explain without notes. Before interviewing, do Part A of `MILESTONES.md`, run the
pipeline yourself, and write at least one extension (T1 is the best story).
