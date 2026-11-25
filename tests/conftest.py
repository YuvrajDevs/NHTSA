"""Test setup: a throw-away lake per test session and one small local Spark session."""
import os
import sys
import tempfile
from pathlib import Path

import pytest

_TMP = tempfile.mkdtemp(prefix="vsi-test-")
os.environ["VSI_LAKE"] = str(Path(_TMP) / "lake")          # must be set before vsi.config is imported
os.environ["VSI_DATA"] = str(Path(_TMP) / "data")
os.environ["VSI_SHUFFLE_PARTITIONS"] = "2"
os.environ["VSI_DRIVER_MEMORY"] = "1g"
os.environ["VSI_SPARK_MASTER"] = "local[2]"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from vsi.spark import get_spark
    return get_spark("vsi-tests")
