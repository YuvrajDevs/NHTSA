"""Enforce the data contracts in contracts/*.yml against silver and gold.

For every table: schema (columns exist, types match), primary key (unique, not null), row counts, freshness,
column rules (nullability, max null %, accepted values, ranges, regex), foreign keys (relationships) and custom SQL
checks. Column-level rules are computed in ONE aggregation pass per table, so checking 2.3M rows stays fast.

Severity: `error` blocks the pipeline (run exits non-zero, Airflow marks the task failed, downstream tasks don't
run); `warn` is reported but doesn't block. Results go to gold._quality_results (history) and docs/quality-report.md.
"""
from datetime import date, datetime, timezone

import yaml
from pyspark.sql import functions as F

from .config import CONTRACTS, OUT_DOCS, SAMPLE_ROWS, table
from .spark import get_spark

TYPE_MAP = {"string": "string", "int": "int", "bigint": "bigint", "date": "date", "boolean": "boolean",
            "double": "double", "timestamp": "timestamp"}


def load_contracts() -> list[dict]:
    out = []
    for f in sorted(CONTRACTS.glob("*.yml")):
        doc = yaml.safe_load(f.read_text())
        for c in doc.get("tables", [doc]):
            c["_file"] = f.name
            out.append(c)
    return out


def read(spark, full_name: str):
    layer, name = full_name.split(".", 1)
    df = spark.read.format("delta").load(table(layer, name))
    return df.where("NOT _is_deleted") if "_is_deleted" in df.columns else df


def check_table(spark, c: dict, run_date: str) -> list[dict]:
    res = []

    def add(check, passed, detail, severity="error", column=None):
        res.append({"table": c["table"], "check": check, "column": column, "passed": bool(passed),
                    "severity": severity, "detail": detail})

    df = read(spark, c["table"])
    dtypes = dict(df.dtypes)
    cols = c.get("columns") or {}

    # ---- schema
    for name, spec in cols.items():
        if name not in dtypes:
            add("column exists", False, f"missing column {name}", column=name)
        elif spec.get("type") and dtypes[name] != TYPE_MAP.get(spec["type"], spec["type"]):
            add("column type", False, f"{name} is {dtypes[name]}, contract says {spec['type']}", column=name)

    # ---- one aggregation pass for row count + every column rule
    aggs = [F.count(F.lit(1)).alias("_rows")]
    rules = []   # (alias, check name, column, spec, kind)
    for name, spec in cols.items():
        if name not in dtypes:
            continue
        col = F.col(name)
        aggs.append(F.sum(col.isNull().cast("int")).alias(f"{name}__nulls"))
        if "accepted_values" in spec:
            aggs.append(F.sum((col.isNotNull() & ~col.cast("string").isin([str(v) for v in spec["accepted_values"]])).cast("int")).alias(f"{name}__accepted"))
            rules.append((f"{name}__accepted", "accepted values", name, spec))
        if "range" in spec:
            lo, hi = spec["range"]
            aggs.append(F.sum((col.isNotNull() & ~col.between(lo, hi)).cast("int")).alias(f"{name}__range"))
            rules.append((f"{name}__range", f"range [{lo}, {hi}]", name, spec))
        if "regex" in spec:
            aggs.append(F.sum((col.isNotNull() & ~col.rlike(spec["regex"])).cast("int")).alias(f"{name}__regex"))
            rules.append((f"{name}__regex", "format (regex)", name, spec))
    for chk in c.get("checks", []):
        alias = f"__chk_{len(aggs)}"
        aggs.append(F.sum(F.expr(f"CASE WHEN {chk['sql_violations']} THEN 1 ELSE 0 END")).alias(alias))
        chk["_alias"] = alias
    r = df.agg(*aggs).collect()[0].asDict()
    rows = r["_rows"] or 0
    pct = lambda n: 100.0 * (n or 0) / rows if rows else 0.0  # noqa: E731

    rc = c.get("row_count") or {}
    if "min" in rc:   # in sample mode (VSI_SAMPLE_ROWS) a small table is expected: report, don't block
        add("row count", rows >= rc["min"], f"{rows:,} rows (min {rc['min']:,})" + (" [sample mode]" if SAMPLE_ROWS else ""),
            "warn" if SAMPLE_ROWS else "error")
    if "equals_table" in rc:
        other = read(spark, rc["equals_table"]).count()
        add("row count reconciles", rows == other, f"{rows:,} rows vs {other:,} in {rc['equals_table']}")
    if rc == {}:
        add("row count", rows > 0, f"{rows:,} rows")

    for name, spec in cols.items():
        if name not in dtypes:
            continue
        nulls = r[f"{name}__nulls"] or 0
        if spec.get("nullable") is False:
            add("not null", nulls == 0, f"{nulls:,} NULLs", column=name)
        elif "max_null_pct" in spec:
            add(f"NULLs <= {spec['max_null_pct']}%", pct(nulls) <= spec["max_null_pct"], f"{pct(nulls):.2f}% NULL",
                spec.get("severity", "warn"), name)
    for alias, check, name, spec in rules:
        bad = r[alias] or 0
        limit = spec.get("max_violation_pct", 0)
        add(check, pct(bad) <= limit, f"{bad:,} violations ({pct(bad):.3f}%, allowed {limit}%)",
            spec.get("severity", "error"), name)
    for chk in c.get("checks", []):
        bad = r[chk["_alias"]] or 0
        limit = chk.get("max_violation_pct", 0)
        add(chk["name"], pct(bad) <= limit, f"{bad:,} rows ({pct(bad):.3f}%, allowed {limit}%)", chk.get("severity", "error"))

    # ---- primary key
    pk = c.get("primary_key")
    if pk:
        dupes = df.groupBy(*pk).count().where("count > 1").count()
        add("primary key unique", dupes == 0, f"{dupes:,} duplicated keys", column=", ".join(pk))

    # ---- relationships (foreign keys): every value must exist in the referenced dimension
    for rel in c.get("relationships", []):
        ref_t, ref_c = rel["references"].rsplit(".", 1)
        ref = read(spark, ref_t).select(F.col(ref_c).alias("_ref"))
        orphans = df.select(rel["column"]).where(F.col(rel["column"]).isNotNull()).join(
            ref, F.col(rel["column"]) == F.col("_ref"), "left_anti").count()
        add("foreign key", orphans == 0, f"{orphans:,} values not in {rel['references']}", column=rel["column"])

    # ---- freshness
    fr = c.get("freshness")
    if fr:
        latest = df.agg(F.max(fr["column"])).collect()[0][0]
        age = (date.fromisoformat(run_date) - latest).days if latest else None
        add(f"freshness <= {fr['max_age_days']} days", age is not None and age <= fr["max_age_days"],
            f"latest {fr['column']} = {latest} ({age} days before {run_date})", fr.get("severity", "warn"), fr["column"])
    return res


def run(run_date: str | None = None) -> bool:
    spark = get_spark()
    run_date = run_date or date.today().isoformat()
    results = []
    for c in load_contracts():
        out = check_table(spark, c, run_date)
        results += out
        bad = [x for x in out if not x["passed"]]
        print(f"quality {c['table']:<36} {len(out) - len(bad):>3}/{len(out)} passed"
              + (f"  ({sum(x['severity'] == 'error' for x in bad)} blocking)" if bad else ""))
    blocking = [x for x in results if not x["passed"] and x["severity"] == "error"]
    now = datetime.now(timezone.utc).isoformat()
    (spark.createDataFrame([{**x, "column": x["column"] or "", "run_date": run_date, "checked_at": now} for x in results])
         .withColumn("run_date", F.col("run_date").cast("date")).withColumn("checked_at", F.col("checked_at").cast("timestamp"))
         .write.format("delta").mode("overwrite").option("replaceWhere", f"run_date = '{run_date}'")
         .partitionBy("run_date").save(table("gold", "_quality_results")))
    write_report(results, run_date)
    print(f"quality: {len(results)} checks, {sum(x['passed'] for x in results)} passed, "
          f"{sum(not x['passed'] and x['severity'] == 'warn' for x in results)} warnings, {len(blocking)} blocking")
    return not blocking


def write_report(results: list[dict], run_date: str):
    lines = [f"# Data quality report ({run_date})", "",
             "Generated by `python -m vsi.cli quality` from the contracts in `contracts/`. ",
             "✅ passed · ⚠️ warning (reported, not blocking) · ❌ blocking (pipeline stops)", ""]
    passed = sum(x["passed"] for x in results)
    lines += [f"**{passed} of {len(results)} checks passed.**", ""]
    for t in dict.fromkeys(x["table"] for x in results):
        lines += [f"## {t}", "", "| | Check | Column | Result |", "|---|---|---|---|"]
        for x in (y for y in results if y["table"] == t):
            icon = "✅" if x["passed"] else ("⚠️" if x["severity"] == "warn" else "❌")
            lines.append(f"| {icon} | {x['check']} | {x['column'] or ''} | {x['detail']} |")
        lines.append("")
    OUT_DOCS.mkdir(parents=True, exist_ok=True)
    (OUT_DOCS / "quality-report.md").write_text("\n".join(lines))
