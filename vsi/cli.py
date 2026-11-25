"""Command-line entry point:  python -m vsi.cli <command> [--date YYYY-MM-DD]

    download         fetch NHTSA files into data/landing/<date>/ (skips unchanged files)
    bronze           land every line as received
    silver           parse, validate, quarantine, detect changes, merge
    gold             build the star schema and data-product marts from sql/gold/*.sql
    quality          run data contracts against silver and gold, write the quality report
    ai               build the AI-ready complaint documents and similarity index
    catalog          generate the data catalog (docs/catalog.md + site/catalog.json)
    run              all of the above for one date (what the Airflow DAG calls, step by step)
    rebuild SOURCE   replay silver for one source from bronze (after fixing a parsing bug)
    verify-changes   compare detected changes with a simulated day's expectations
    search TEXT      find complaints similar to a description (uses the AI index)
"""
import argparse
import json
import sys
from datetime import date

from .config import LANDING
from .sources import ALL

STEPS = ("download", "bronze", "silver", "gold", "quality", "ai", "catalog")


def step(name: str, load_date: str, sources: list[str], no_download: bool = False):
    if name == "download":
        if no_download or (LANDING / load_date / "_manifest.json").exists() or (LANDING / load_date / "_expected_changes.json").exists():
            print(f"download: landing/{load_date} already present, skipping")
            return
        from .download import download
        download(load_date, [ALL[s] for s in sources])
    elif name == "bronze":
        from . import bronze
        for s in sources:
            bronze.load(load_date, s)
    elif name == "silver":
        from . import silver
        for s in sources:
            silver.load(load_date, s)
    elif name == "gold":
        from . import gold
        gold.build(load_date)
    elif name == "quality":
        from . import quality
        ok = quality.run(load_date)
        if not ok:
            raise SystemExit("quality: blocking data-contract violations, see docs/quality-report.md")
    elif name == "ai":
        from . import ai
        ai.build(load_date)
    elif name == "catalog":
        from . import catalog
        catalog.build(load_date)


def main(argv=None):
    p = argparse.ArgumentParser(prog="vsi", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=[*STEPS, "run", "rebuild", "verify-changes", "search"])
    p.add_argument("arg", nargs="?", help="source name for rebuild, text for search")
    p.add_argument("--date", default=date.today().isoformat(), help="load date (Airflow passes the logical date)")
    p.add_argument("--sources", nargs="*", default=list(ALL))
    p.add_argument("--no-download", action="store_true")
    a = p.parse_args(argv)

    if a.command in STEPS:
        step(a.command, a.date, a.sources, a.no_download)
    elif a.command == "run":
        for s in STEPS:
            print(f"== {s}")
            step(s, a.date, a.sources, a.no_download)
    elif a.command == "rebuild":
        from . import silver
        silver.rebuild(a.arg)
    elif a.command == "verify-changes":
        from . import verify
        sys.exit(0 if verify.changes(a.date) else 1)
    elif a.command == "search":
        from . import ai
        print(json.dumps(ai.search(a.arg), indent=2))


if __name__ == "__main__":
    main()
