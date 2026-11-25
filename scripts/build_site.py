"""Build site/data.json for the project page from the gold tables (no Spark needed: reads Delta with delta-rs).

    python scripts/build_site.py
"""
import json
import sys
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from vsi.config import DOCS, SITE, table  # noqa: E402


def gold(name, columns=None):
    return DeltaTable(table("gold", name)).to_pandas(columns=columns)


def main():
    ew = gold("mart_recall_early_warning")
    ew = ew[~ew["campaign_id"].str.startswith("26V9")]          # never publish simulated campaigns
    bins = [(100_000, None, "100,000+"), (10_000, 100_000, "10,000-100,000"), (1_000, 10_000, "1,000-10,000"), (0, 1_000, "under 1,000")]
    by_size = []
    for lo, hi, label in bins:
        s = ew[(ew.potentially_affected.fillna(0) >= lo) & ((ew.potentially_affected.fillna(0) < hi) if hi else True)]
        by_size.append({"size": label, "recalls": int(len(s)),
                        "pct_prior": round(100 * s.had_prior_complaints.mean(), 1),
                        "pct_spike": round(100 * s.had_spike_signal.mean(), 1),
                        "median_days_first": None if s.days_first_complaint_to_recall.dropna().empty else int(s.days_first_complaint_to_recall.median()),
                        "median_spike_lead": None if s.days_spike_to_recall.dropna().empty else int(s.days_spike_to_recall.median())})
    by_initiator = [{"influenced_by": k, "recalls": int(len(g)), "pct_prior": round(100 * g.had_prior_complaints.mean(), 1)}
                    for k, g in ew.groupby("influenced_by") if len(g) > 100]

    # Trend explorer: the make x system pairs with the most large recalls preceded by complaints
    risk = gold("mart_component_risk_monthly", ["make", "component_system", "month_start", "complaints", "is_spike",
                                                "baseline_avg_12m"])
    risk = risk[risk.month_start >= pd.Timestamp("2000-01-01").date()]
    rec = DeltaTable(table("silver", "recalls")).to_pandas(columns=["campaign_id", "make", "component_system", "_is_deleted"])
    rec = rec[~rec._is_deleted].drop_duplicates(["campaign_id", "make", "component_system"])
    big = ew[ew.potentially_affected.fillna(0) >= 100_000][["campaign_id", "report_received_date", "potentially_affected", "had_spike_signal"]]
    big = big.merge(rec, on="campaign_id")
    top = (risk.groupby(["make", "component_system"]).complaints.sum().reset_index()
           .merge(big.groupby(["make", "component_system"]).campaign_id.nunique().rename("big_recalls").reset_index(),
                  on=["make", "component_system"]))
    top = top.sort_values(["big_recalls", "complaints"], ascending=False).head(40)
    series = []
    for _, t in top.iterrows():
        r = risk[(risk.make == t.make) & (risk.component_system == t.component_system)].sort_values("month_start")
        b = big[(big.make == t.make) & (big.component_system == t.component_system)].drop_duplicates("campaign_id")
        series.append({
            "make": t.make, "system": t.component_system, "start": str(r.month_start.iloc[0])[:7],
            "complaints": r.complaints.astype(int).tolist(),
            "spikes": [i for i, v in enumerate(r.is_spike.tolist()) if v],
            "recalls": [{"id": x.campaign_id, "date": str(x.report_received_date), "units": int(x.potentially_affected or 0),
                         "flagged": bool(x.had_spike_signal)} for x in b.itertuples()],
        })

    q = DeltaTable(table("gold", "_quality_results")).to_pandas()
    q = q[q.run_date == q.run_date.max()]
    quality = {"checks": int(len(q)), "passed": int(q.passed.sum()),
               "warnings": int((~q.passed & (q.severity == "warn")).sum()),
               "blocking": int((~q.passed & (q.severity == "error")).sum()),
               "tables": int(q["table"].nunique()), "run_date": str(q.run_date.max())}
    ai = json.loads((DOCS / "ai-readiness.json").read_text())
    perf = json.loads((DOCS / "perf-results.json").read_text()) if (DOCS / "perf-results.json").exists() else None
    examples = []
    try:
        from vsi.ai import search
        for qtext in ("brake pedal went to the floor and the car would not stop",
                      "airbag did not deploy in a frontal crash"):
            examples.append({"query": qtext, "results": search(qtext, 3)})
    except FileNotFoundError:
        pass
    out = {"examples": examples, "early_warning": {"recalls": int(len(ew)), "by_size": by_size, "by_initiator": by_initiator},
           "series": series, "quality": quality, "ai": ai, "perf": perf}
    SITE.mkdir(exist_ok=True)
    (SITE / "data.json").write_text(json.dumps(out, default=str, separators=(",", ":")))
    print(f"wrote site/data.json ({(SITE / 'data.json').stat().st_size // 1024} KB, {len(series)} series)")


if __name__ == "__main__":
    main()
