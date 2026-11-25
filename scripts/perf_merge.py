"""Performance experiment: the daily MERGE into silver.complaints (day 1 snapshot, then day 2 with ~750 changes).

Runs in its own lake (lake_perf/) so real tables are untouched, then reads each MERGE's own metrics from the Delta
transaction log: files rewritten, rows copied, bytes written, time. Writes docs/perf-results.json.

    python scripts/perf_merge.py --day1 2026-11-25 --day2 2026-11-26 [--deletion-vectors]
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "lake_perf"


def run(args, env):
    t = time.time()
    r = subprocess.run([sys.executable, "-m", "vsi.cli", *args], cwd=ROOT, env=env, capture_output=True, text=True)
    if r.returncode:                      # show the real error instead of hiding it (build-log F12)
        print(r.stdout[-3000:], r.stderr[-6000:], sep="\n")
        raise SystemExit(f"step failed: {' '.join(args)}")
    return round(time.time() - t, 1)


def merges(path):
    from deltalake import DeltaTable
    keys = ("numTargetRowsUpdated", "numTargetRowsInserted", "numTargetFilesRemoved", "numTargetFilesAdded",
            "numTargetRowsCopied", "numTargetBytesAdded", "numTargetDeletionVectorsAdded", "executionTimeMs", "rewriteTimeMs")
    return [{k: int(h["operationMetrics"].get(k, 0)) for k in keys}
            for h in reversed(DeltaTable(path).history()) if h.get("operation") == "MERGE"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day1", default="2026-11-25")
    ap.add_argument("--day2", default="2026-11-26")
    ap.add_argument("--deletion-vectors", action="store_true")
    ap.add_argument("--label", default="single_merge")
    a = ap.parse_args()
    shutil.rmtree(LAKE, ignore_errors=True)
    env = os.environ | {"VSI_LAKE": str(LAKE), "VSI_DELETION_VECTORS": str(a.deletion_vectors).lower()}
    for d in (a.day1, a.day2):
        run(["bronze", "--date", d, "--sources", "complaints"], env)
    t1 = run(["silver", "--date", a.day1, "--sources", "complaints"], env)
    t2 = run(["silver", "--date", a.day2, "--sources", "complaints"], env)
    res = {"day1_seconds": t1, "day2_seconds": t2, "merges": merges(str(LAKE / "silver" / "complaints"))}
    out = ROOT / "docs" / "perf-results.json"
    allres = json.loads(out.read_text()) if out.exists() else {}
    allres[a.label] = res
    out.write_text(json.dumps(allres, indent=2))
    print(json.dumps(res, indent=2))
    shutil.rmtree(LAKE, ignore_errors=True)


if __name__ == "__main__":
    main()
