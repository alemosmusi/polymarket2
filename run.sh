#!/usr/bin/env bash
set -euo pipefail

cd /app

mkdir -p data

STOP_TRACKING_DAYS_AFTER_EVENT="${STOP_TRACKING_DAYS_AFTER_EVENT:-1}"
TRACKER_TIMEZONE="${TRACKER_TIMEZONE:-Europe/Madrid}"
FAST_START_HOUR="${FAST_START_HOUR:-4}"
FAST_END_HOUR="${FAST_END_HOUR:-5}"
FAST_END_MINUTE="${FAST_END_MINUTE:-30}"
ACTIVE_END_HOUR="${ACTIVE_END_HOUR:-10}"
SLOW_INTERVAL_MINUTES="${SLOW_INTERVAL_MINUTES:-5}"

CURRENT_HOUR="$(TZ="${TRACKER_TIMEZONE}" date +%H)"
CURRENT_MINUTE="$(TZ="${TRACKER_TIMEZONE}" date +%M)"
CURRENT_TOTAL_MINUTES=$((10#${CURRENT_HOUR} * 60 + 10#${CURRENT_MINUTE}))
FAST_START_TOTAL_MINUTES=$((10#${FAST_START_HOUR} * 60))
FAST_END_TOTAL_MINUTES=$((10#${FAST_END_HOUR} * 60 + 10#${FAST_END_MINUTE}))
ACTIVE_END_TOTAL_MINUTES=$((10#${ACTIVE_END_HOUR} * 60 + 59))

SHOULD_RUN=0
if (( CURRENT_TOTAL_MINUTES >= FAST_START_TOTAL_MINUTES && CURRENT_TOTAL_MINUTES <= FAST_END_TOTAL_MINUTES )); then
  SHOULD_RUN=1
elif (( CURRENT_TOTAL_MINUTES > FAST_END_TOTAL_MINUTES && CURRENT_TOTAL_MINUTES <= ACTIVE_END_TOTAL_MINUTES )); then
  if (( 10#${CURRENT_MINUTE} % 10#${SLOW_INTERVAL_MINUTES} == 0 )); then
    SHOULD_RUN=1
  fi
fi

if (( SHOULD_RUN == 0 )); then
  echo "Skipping run at ${CURRENT_HOUR}:${CURRENT_MINUTE} ${TRACKER_TIMEZONE}"
  exit 0
fi

python polymarket_highest_temp_tracker_v2.py \
  --db data/polymarket_highest_temp.db \
  --batch-size 25 \
  --stop-tracking-days-after-event "${STOP_TRACKING_DAYS_AFTER_EVENT}" \
  run-once

python - <<'PY'
import sqlite3
import sys

db_path = "data/polymarket_highest_temp.db"
required_market_columns = {
    "initial_no_midpoint",
    "initial_no_bid",
    "initial_no_ask",
    "latest_no_midpoint",
    "latest_no_bid",
    "latest_no_ask",
}
required_snapshot_columns = {"no_midpoint", "no_bid", "no_ask"}

with sqlite3.connect(db_path) as conn:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    if "forecast_positions" not in tables:
        raise SystemExit("Sanity check failed: forecast_positions table is missing")

    market_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(markets)")
    }
    snapshot_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(snapshots)")
    }
    missing_market_columns = sorted(required_market_columns - market_columns)
    missing_snapshot_columns = sorted(required_snapshot_columns - snapshot_columns)
    if missing_market_columns:
        raise SystemExit(
            "Sanity check failed: markets is missing NO columns: "
            + ", ".join(missing_market_columns)
        )
    if missing_snapshot_columns:
        raise SystemExit(
            "Sanity check failed: snapshots is missing NO columns: "
            + ", ".join(missing_snapshot_columns)
        )

    distinct_event_dates = conn.execute(
        """
        SELECT COUNT(DISTINCT event_date_iso)
        FROM events
        WHERE event_date_iso IS NOT NULL
        """
    ).fetchone()[0]
    if distinct_event_dates > 1:
        raise SystemExit(
            f"Sanity check failed: expected only one event_date_iso, found {distinct_event_dates}"
        )

    tracked_dates = conn.execute(
        """
        SELECT COUNT(DISTINCT e.event_date_iso)
        FROM snapshots s
        JOIN events e ON e.event_id = s.event_id
        WHERE e.event_date_iso IS NOT NULL
        """
    ).fetchone()[0]
    if tracked_dates > 1:
        raise SystemExit(
            f"Sanity check failed: snapshots still contain {tracked_dates} event dates"
        )

print("Sanity check OK: explicit NO columns and latest-day-only data are present")
PY

python polymarket_highest_temp_tracker_v2.py --db data/polymarket_highest_temp.db export-csv --out data/snapshots.csv
python polymarket_highest_temp_tracker_v2.py --db data/polymarket_highest_temp.db export-picks-csv --out data/picks.csv
python polymarket_highest_temp_tracker_v2.py --db data/polymarket_highest_temp.db export-forecast-positions-csv --out data/forecast_positions.csv

TMP_REPO="/tmp/repo"
rm -rf "$TMP_REPO"

git clone "https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git" "$TMP_REPO"

cd "$TMP_REPO"

git config user.name "${GIT_USER_NAME}"
git config user.email "${GIT_USER_EMAIL}"

mkdir -p data
cp /app/data/snapshots.csv data/
cp /app/data/picks.csv data/
cp /app/data/forecast_positions.csv data/
rm -f data/polymarket_highest_temp.db

git add -A data/

if git diff --cached --quiet; then
  echo "No changes to commit"
  exit 0
fi

git commit -m "Update tracker data"
git push origin "main"
