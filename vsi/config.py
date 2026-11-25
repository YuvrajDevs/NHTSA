"""Central configuration. Every path and setting can be overridden with an environment variable."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("VSI_ROOT", Path(__file__).resolve().parents[1]))
DATA = Path(os.environ.get("VSI_DATA", ROOT / "data"))
LANDING = DATA / "landing"          # downloaded zips, one folder per load date, never modified
LAKE = Path(os.environ.get("VSI_LAKE", ROOT / "lake"))
CONTRACTS = ROOT / "contracts"
SQL_DIR = ROOT / "sql"
DOCS = ROOT / "docs"
SITE = ROOT / "site"

# Generated outputs (quality report, catalog, AI index). A run against a separate lake (tests, Airflow sample runs,
# experiments) writes them next to that lake, so it can never overwrite the real reports in docs/ and site/.
_DEFAULT_LAKE = ROOT / "lake"
if LAKE.resolve() == _DEFAULT_LAKE.resolve():
    OUT_DOCS, OUT_SITE, AI_DIR = DOCS, SITE, DATA / "ai"
else:
    OUT_DOCS, OUT_SITE, AI_DIR = LAKE / "_outputs" / "docs", LAKE / "_outputs" / "site", LAKE / "_outputs" / "ai"

SPARK_MASTER = os.environ.get("VSI_SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.environ.get("VSI_DRIVER_MEMORY", "6g")
SHUFFLE_PARTITIONS = int(os.environ.get("VSI_SHUFFLE_PARTITIONS", "16"))

# Deletion vectors let MERGE mark changed rows instead of rewriting files. OFF by default: with Delta 4.0 the MERGE
# on silver.complaints ran out of memory while planning (build-log F11). Turn on to experiment.
DELETION_VECTORS = os.environ.get("VSI_DELETION_VECTORS", "false").lower() == "true"

# Optional: limit rows per source for fast local runs and CI (0 = everything)
SAMPLE_ROWS = int(os.environ.get("VSI_SAMPLE_ROWS", "0"))


def table(layer: str, name: str) -> str:
    """Filesystem path of a Delta table, e.g. table('silver', 'complaints')."""
    return str(LAKE / layer / name)
