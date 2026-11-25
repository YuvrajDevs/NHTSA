#!/usr/bin/env bash
# End-to-end demo on real NHTSA data, then a simulated second day with known changes, verified exactly.
# Needs: Python venv with requirements.txt, Java 17. Takes ~25 min on a 4-core laptop (2.3M complaints, twice).
set -euo pipefail
cd "$(dirname "$0")/.."
DAY1=${DAY1:-$(date +%F)}
DAY2=$(python -c "import datetime as d; print(d.date.fromisoformat('$DAY1') + d.timedelta(days=1))")

echo "== 1. full pipeline on today's real snapshot ($DAY1): download -> bronze -> silver -> gold -> quality -> ai -> catalog"
python -m vsi.cli run --date "$DAY1"

echo "== 2. re-run the same day: silver skips it (idempotent), gold/quality re-run harmlessly"
python -m vsi.cli silver --date "$DAY1"

echo "== 3. simulate $DAY2 from $DAY1 with KNOWN changes (new, edited, removed records + broken lines)"
python scripts/simulate_next_day.py --date "$DAY1"

echo "== 4. process $DAY2 and prove change detection is exact"
python -m vsi.cli bronze --date "$DAY2"
python -m vsi.cli silver --date "$DAY2"
python -m vsi.cli verify-changes --date "$DAY2"

echo "== 5. try the AI layer"
python -m vsi.cli search "brake pedal went to the floor and the car would not stop"
echo "Done. Quality report: docs/quality-report.md · Catalog: docs/catalog.md"
echo "Note: the simulated day adds a few hundred synthetic records; run 'rm -rf lake' and step 1 again for a real-only lake."
