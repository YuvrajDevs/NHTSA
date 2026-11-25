# Architecture decision records (short form)

**ADR-001 Medallion layers on Delta Lake.** Bronze = every line as received (replayable), silver = typed current state
with change history, gold = star schema + data products. Two real parsing bugs (build-log F4, F5) were fixed by
replaying silver from bronze without re-downloading anything. Alternative: load straight into typed tables (no
replay, every bug fix needs the source again, and NHTSA only serves today's file).

**ADR-002 PySpark for bronze/silver, SQL files for gold.** The 1.6 GB complaints file needs a distributed engine for
parsing and change detection; the gold layer is modelling, which reads best as SQL that analysts can review. Each
`sql/gold/NN_name.sql` is one SELECT; the runner materialises it as a Delta table. Alternative: dbt (same idea with
more tooling; the SQL here ports to dbt models one-to-one).

**ADR-003 Daily full snapshot -> change data capture by row hash.** NHTSA publishes full files, not deltas, and its
own docs say ids and names can change. Comparing each snapshot with silver on the business key + a SHA-256 of all
columns yields inserts, updates and soft deletes; every change is logged. Proven exact on a simulated day with known
changes (`verify-changes`). Alternative: overwrite silver daily (no history, no idea what changed).

**ADR-004 Hash-based surrogate keys.** `xxhash64(natural key)` instead of sequences: gold is rebuilt in full each run
and keys come out identical every time, with no key-mapping table to maintain. Collision risk at 100k rows is
negligible (64-bit). Unknown member = -1 in every dimension.

**ADR-005 Soft deletes in silver, SCD2 only for recalls.** A complaint that disappears is flagged, not removed (it
may be a NHTSA correction worth auditing). Recalls get full version history because unit counts and descriptions
are revised after filing and "what did this recall say on date X" is a real question. Complaints don't get SCD2:
history of consumer text has little analytical value and would double 2.3M rows of storage.

**ADR-006 Data contracts as YAML, enforced at runtime.** One file per table: schema, keys, rules, freshness, owner,
PII tags. Blocking violations fail the Airflow quality gate so bad data never reaches consumers; warnings are reported.
The same files generate the data catalog, so documentation can't drift from what is enforced.

**ADR-007 Quarantine instead of failing or dropping.** Unparseable lines and duplicate keys go to
`silver._quarantine` with a reason code and the raw line. One bad row never stops 2.3M good ones, and nothing
disappears silently.

**ADR-008 A load log makes runs idempotent and ordered.** `silver._load_log` records each (source, load date).
Re-running a processed date is a no-op; an older date than the last one is refused (applying Tuesday's snapshot
after Wednesday's would "undo" Wednesday's changes). Full replays go through `rebuild`.

**ADR-009 Airflow orchestrates, the package does the work.** Every task is `python -m vsi.cli <step> --date {{ ds }}`.
Logic is unit-tested without Airflow, and the same commands run by hand, in CI or in any other orchestrator
(Azure Data Factory, Databricks Workflows). Spark tasks share a one-slot pool on a single machine.

**ADR-010 AI-ready documents are a separate, scrubbed gold table.** The raw narrative stays in silver (tagged as PII);
`gold.complaint_documents` replaces phones, emails, VINs and links with placeholders and adds metadata. A TF-IDF index
proves the table is usable for retrieval with no external API or keys; swapping in an embedding model changes one function.

**ADR-011 One MERGE per load, deletion vectors off.** A MERGE rewrites every data file holding a matched row, and a
day's few hundred changes are scattered across all files, so each MERGE rewrites the whole 900 MB table. Inserts,
updates and soft deletes therefore go through ONE MERGE (deletes ride along with an `_op` flag), halving the rewrite
(docs/performance.md). Deletion vectors would avoid the rewrite entirely, but with Delta 4.0 the MERGE ran out of
memory while planning (build-log F11), so they stay behind a setting (`VSI_DELETION_VECTORS`).

**ADR-012 Public data, local-first, no accounts.** Everything runs with Docker + Python + Java on a laptop. The same
code runs on Databricks by changing the Spark master and storage paths (see MILESTONES T5).
