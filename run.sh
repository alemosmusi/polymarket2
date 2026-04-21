#!/usr/bin/env bash
set -euo pipefail

cd /app

mkdir -p data

STOP_TRACKING_DAYS_AFTER_EVENT="${STOP_TRACKING_DAYS_AFTER_EVENT:-1}"

python polymarket_highest_temp_tracker_v2.py \
  --db data/polymarket_highest_temp.db \
  --batch-size 25 \
  --stop-tracking-days-after-event "${STOP_TRACKING_DAYS_AFTER_EVENT}" \
  run-once
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
cp /app/data/polymarket_highest_temp.db data/
cp /app/data/snapshots.csv data/
cp /app/data/picks.csv data/
cp /app/data/forecast_positions.csv data/

git add data/

if git diff --cached --quiet; then
  echo "No changes to commit"
  exit 0
fi

git commit -m "Update tracker data"
git push origin "main"
