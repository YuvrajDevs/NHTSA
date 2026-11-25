# Performance: the daily MERGE

Measured with `scripts/perf_merge.py` (4-core laptop-class machine, local Spark). Numbers come from Delta's own
transaction log (`DeltaTable(path).history()` -> `operationMetrics`), so they are not stopwatch guesses.

## The problem
Each day NHTSA republishes all 2.3M complaint rows. Change detection finds the few hundred that changed (here 750:
250 new, 250 edited, 250 removed) and MERGEs them into `silver.complaints`. But a Delta MERGE **rewrites every data
file that contains a matched row**, and a day's changes are scattered across the whole table. With 16 files of ~55 MB,
every file contains at least one changed row, so every MERGE rewrites the entire table.

The first version used two MERGEs (inserts/updates, then soft deletes), so it rewrote the table **twice**.

## The fix: one MERGE
Deleted keys ride along in the same source DataFrame with an `_op = 'delete'` flag; conditional clauses route each
row (`whenMatchedUpdate(condition="s._op = 'upsert'")`, `whenMatchedUpdate(condition="s._op = 'delete'")`,
`whenNotMatchedInsert(condition="s._op = 'upsert'")`). Same result, verified by the silver tests and `verify-changes`.

| Day-2 update of silver.complaints | Before: 2 MERGEs | After: 1 MERGE |
|---|---|---|
| Table rewrites | 2 | **1** |
| Data files rewritten | 32 (16 + 16) | **16** |
| Unchanged rows copied into new files | 4,540,340 | **2,269,795** |
| Data written | 1.79 GB | **0.89 GB** |
| MERGE execution time | 70.3 s | **43.8 s** (-38%) |

## What I tried that didn't work: deletion vectors
Deletion vectors let a MERGE mark rows as removed instead of rewriting files, which should cut the rewrite to almost
nothing. With Delta 4.0.0 on this table, the day-2 MERGE ran out of Java heap **while planning the query** (stack in
Catalyst `transformDownWithPruning`), with both a 6 GB and a 10 GB driver (build-log F11). They stay behind a
setting (`VSI_DELETION_VECTORS=true`) for anyone who wants to retry on a newer Delta version.

## Where the rest of the time goes (day 2, complaints, 328 s in total)
Parsing 2.3M bronze lines and hashing them, the full outer join against silver to detect changes, the MERGE (44 s),
and the reconciliation counts. Next optimisations (Milestone T3): compare bronze **line hashes** between days first,
so only changed lines are parsed (~750 instead of 2.3M); and cluster silver by received date so the MERGE touches
fewer files when changes are mostly recent.
