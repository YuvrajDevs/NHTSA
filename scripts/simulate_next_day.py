"""Create a realistic 'next day' NHTSA snapshot with KNOWN changes, to prove change detection is exact.

NHTSA only publishes today's full snapshot, so to test day-2 behaviour we derive one from day 1:
  complaints: N new complaints, N edited (narrative / injuries), N removed, plus malformed lines
  recalls:    N new records, N edited (units affected revised, summary corrected), N removed
The script writes data/landing/<next_date>/ (same file names, re-zipped) and an expectations file. Then:
    python -m vsi.cli run --date <next_date>
    python -m vsi.cli verify-changes --date <next_date>      # compares detected changes with the expectations
Deterministic (seeded), so the same input always gives the same changes.
"""
import argparse
import json
import random
import shutil
import sys
import zipfile
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from vsi.config import LANDING  # noqa: E402
from vsi.sources import ALL  # noqa: E402


def rewrite(src_zip: Path, dst_zip: Path, mutate):
    with zipfile.ZipFile(src_zip) as zin, zipfile.ZipFile(dst_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as zout:
        for name in zin.namelist():
            with zin.open(name) as f:
                lines = f.read().decode("latin-1").split("\n")
            lines = mutate(name, lines)
            zout.writestr(name, "\n".join(lines).encode("latin-1"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-11-25", help="existing load date to derive from")
    ap.add_argument("--n", type=int, default=250, help="changes of each kind per source")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    src_dir = LANDING / a.date
    nxt = (date.fromisoformat(a.date) + timedelta(days=1)).isoformat()
    out = LANDING / nxt
    out.mkdir(parents=True, exist_ok=True)
    expected = {"from": a.date, "load_date": nxt, "complaints": {}, "recalls": {}}

    # ---- complaints: fields 0 = CMPLID, 9 = INJURED, 19 = narrative
    def complaints(name, lines):
        body = [ln for ln in lines if ln.strip()]
        idx = rnd.sample(range(len(body)), 2 * a.n)
        delete, edit = set(idx[: a.n]), idx[a.n:]
        edited = []
        for i in edit:
            f = body[i].rstrip("\r").split("\t")
            f[19] = (f[19] + " UPDATE: DEALER CONFIRMED THE FAILURE AND REPLACED THE PART.")[:2048]
            f[9] = str(int(f[9] or 0) + 1)
            body[i] = "\t".join(f)
            edited.append(f[0])
        removed = [body[i].split("\t")[0] for i in sorted(delete)]
        kept = [ln for i, ln in enumerate(body) if i not in delete]
        new = []
        max_id = max(int(ln.split("\t")[0]) for ln in body if ln.split("\t")[0].isdigit())
        for k in range(a.n):
            f = body[rnd.randrange(len(body))].rstrip("\r").split("\t")
            f[0] = str(max_id + 1 + k)
            f[1] = str(99000000 + k)
            f[16] = nxt.replace("-", "")
            f[15] = nxt.replace("-", "")
            kept.append("\t".join(f))
            new.append(f[0])
        kept += ["THIS LINE IS BROKEN", "123\tONLY\tTHREE FIELDS", ""]
        expected["complaints"] = {"I": sorted(new), "U": sorted(edited), "D": sorted(removed), "quarantined": 2}
        return kept

    # ---- recalls: fields 0 = RECORD_ID, 11 = POTAFF, 19 = defect summary
    rc_state = {"I": [], "U": [], "D": []}

    def recalls(name, lines):
        body = [ln for ln in lines if ln.strip()]
        if "POST_2010" not in name:
            return body
        idx = rnd.sample(range(len(body)), 2 * a.n)
        delete, edit = set(idx[: a.n]), idx[a.n:]
        for i in edit:
            f = body[i].rstrip("\r").split("\t")
            f[11] = str(int(f[11] or 0) + 1000)
            f[19] = f[19] + " (REVISED)"
            body[i] = "\t".join(f)
            rc_state["U"].append(f[0])
        rc_state["D"] = [body[i].split("\t")[0] for i in sorted(delete)]
        kept = [ln for i, ln in enumerate(body) if i not in delete]
        max_id = max(int(ln.split("\t")[0]) for ln in body)
        for k in range(a.n):
            f = body[rnd.randrange(len(body))].rstrip("\r").split("\t")
            f[0] = str(max_id + 1 + k)
            f[1] = f"26V{900 + k // 50:03d}000"
            kept.append("\t".join(f))
            rc_state["I"].append(f[0])
        return kept

    for src, fn in (("complaints", complaints), ("recalls", recalls)):
        for rel in ALL[src].files:
            nm = Path(rel).name
            print(f"writing {out / nm}")
            rewrite(src_dir / nm, out / nm, fn)
        shutil.copy2(src_dir / Path(ALL[src].doc).name, out)
    expected["recalls"] = {k: sorted(v) for k, v in rc_state.items()} | {"quarantined": 0}
    for f in ALL["investigations"].files + (ALL["investigations"].doc,):       # unchanged source
        shutil.copy2(src_dir / Path(f).name, out)
    (out / "_expected_changes.json").write_text(json.dumps(expected))
    print(f"{nxt}: " + ", ".join(f"{s} " + "/".join(f"{k}={len(v) if isinstance(v, list) else v}" for k, v in expected[s].items())
                                   for s in ("complaints", "recalls")))


if __name__ == "__main__":
    main()
