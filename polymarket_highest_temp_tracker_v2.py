
#!/usr/bin/env python3
"""Track new Polymarket "Highest temperature in ..." events every 5 minutes.

Features
--------
- Polls Polymarket active events and keeps only titles that start with
  "Highest temperature in ".
- Supports aligned polling at minutes 1, 6, 11, 16, ... by default.
- Stores every binary child market (one outcome label per market) in SQLite.
- Captures current YES and NO midpoint/best bid/best ask, and YES spread.
- Tracks only newly discovered events by default, then snapshots them every run.
- When an event is first seen, fetches a "forecast pick":
  1) best effort parse from the Polymarket event page AI summary
  2) fallback to Open-Meteo geocoding + forecast API
- Chooses the child outcome that matches the initial forecasted max temp
  and tracks whether buying that outcome at the first observed ask and
  selling later at the bid would have produced a profit.

Important
---------
- "Launch" / "entry" prices are the first prices *your tracker* saw.
- Polymarket's resolution source for these markets is typically a
  Wunderground history page. The forecast reference in the market page AI
  summary is informational only and does not control resolution.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import logging
import math
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Sequence
from urllib.parse import urlencode, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
POLYMARKET_BASE = "https://polymarket.com"
OPEN_METEO_GEOCODING = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"

DEFAULT_DB = "polymarket_highest_temp.db"
DEFAULT_PAGE_SIZE = 500
DEFAULT_BATCH_SIZE = 100
DEFAULT_POLL_MINUTES = 5
DEFAULT_ALIGN_START_MINUTE = 1
DEFAULT_STOP_TRACKING_DAYS_AFTER_EVENT = 1
TITLE_PREFIX = "highest temperature in "


@dataclass
class EventRecord:
    event_id: str
    title: str
    slug: str | None
    end_date: str | None
    tags: list[str]
    raw_event: dict[str, Any]
    event_date_iso: str | None
    city: str | None
    url: str | None


@dataclass
class MarketRecord:
    event_id: str
    event_title: str
    event_slug: str | None
    event_end_date: str | None
    event_date_iso: str | None
    event_city: str | None
    market_id: str
    market_slug: str | None
    question: str
    condition_id: str | None
    yes_token_id: str | None
    no_token_id: str | None
    outcome_label: str
    unit: str | None
    lower_bound: float | None
    upper_bound: float | None
    tags: list[str]
    raw_event: dict[str, Any]
    raw_market: dict[str, Any]


@dataclass
class ForecastInfo:
    method: str
    source_name: str | None
    source_url: str | None
    forecast_text: str | None
    target_value_raw: float | None
    target_unit: str | None
    target_value_market_unit: float | None
    market_unit: str | None
    city: str | None
    event_date_iso: str | None
    latitude: float | None
    longitude: float | None
    timezone_name: str | None
    raw_json: dict[str, Any]


class HighestTemperatureTracker:
    def __init__(
        self,
        db_path: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        timeout_seconds: int = 20,
    ) -> None:
        self.db_path = db_path
        self.page_size = max(1, min(page_size, 500))
        self.batch_size = max(1, batch_size)
        self.timeout_seconds = timeout_seconds
        self.session = self._build_session()
        self._init_db()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "polymarket-highest-temp-tracker/2.0"})
        return session

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    slug TEXT,
                    event_url TEXT,
                    city TEXT,
                    event_date_iso TEXT,
                    end_date TEXT,
                    tags_json TEXT NOT NULL,
                    unit TEXT,
                    station_name TEXT,
                    resolution_source_url TEXT,
                    tracking_active INTEGER NOT NULL DEFAULT 0,
                    tracking_started_at_utc TEXT,
                    tracking_stopped_at_utc TEXT,
                    first_seen_at_utc TEXT NOT NULL,
                    last_seen_at_utc TEXT NOT NULL,
                    raw_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS markets (
                    market_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    market_slug TEXT,
                    question TEXT NOT NULL,
                    condition_id TEXT,
                    yes_token_id TEXT,
                    no_token_id TEXT,
                    outcome_label TEXT NOT NULL,
                    unit TEXT,
                    lower_bound REAL,
                    upper_bound REAL,
                    tags_json TEXT NOT NULL,
                    first_seen_at_utc TEXT NOT NULL,
                    last_seen_at_utc TEXT NOT NULL,
                    initial_yes_midpoint REAL,
                    initial_yes_bid REAL,
                    initial_yes_ask REAL,
                    initial_no_midpoint REAL,
                    initial_no_bid REAL,
                    initial_no_ask REAL,
                    latest_yes_midpoint REAL,
                    latest_yes_bid REAL,
                    latest_yes_ask REAL,
                    latest_no_midpoint REAL,
                    latest_no_bid REAL,
                    latest_no_ask REAL,
                    latest_spread REAL,
                    active INTEGER NOT NULL DEFAULT 1,
                    raw_json TEXT NOT NULL,
                    FOREIGN KEY(event_id) REFERENCES events(event_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at_utc TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    market_id TEXT NOT NULL,
                    yes_token_id TEXT,
                    yes_midpoint REAL,
                    yes_bid REAL,
                    yes_ask REAL,
                    no_midpoint REAL,
                    no_bid REAL,
                    no_ask REAL,
                    spread REAL,
                    UNIQUE(captured_at_utc, market_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_picks (
                    event_id TEXT PRIMARY KEY,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    forecast_method TEXT,
                    forecast_source_name TEXT,
                    forecast_source_url TEXT,
                    forecast_text TEXT,
                    forecast_target_raw REAL,
                    forecast_target_unit TEXT,
                    forecast_target_market_unit REAL,
                    market_unit TEXT,
                    latitude REAL,
                    longitude REAL,
                    timezone_name TEXT,
                    picked_market_id TEXT,
                    picked_outcome_label TEXT,
                    entry_yes_midpoint REAL,
                    entry_yes_bid REAL,
                    entry_yes_ask REAL,
                    latest_yes_midpoint REAL,
                    latest_yes_bid REAL,
                    latest_yes_ask REAL,
                    latest_spread REAL,
                    best_mid_seen REAL,
                    best_mid_seen_at_utc TEXT,
                    best_exit_bid_seen REAL,
                    best_exit_bid_seen_at_utc TEXT,
                    gross_pnl_if_exit_now REAL,
                    gross_pnl_at_best_exit REAL,
                    raw_json TEXT,
                    FOREIGN KEY(event_id) REFERENCES events(event_id),
                    FOREIGN KEY(picked_market_id) REFERENCES markets(market_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_positions (
                    market_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    forecast_stance TEXT NOT NULL,
                    target_market_id TEXT,
                    target_outcome_label TEXT,
                    entry_midpoint REAL,
                    entry_bid REAL,
                    entry_ask REAL,
                    latest_midpoint REAL,
                    latest_bid REAL,
                    latest_ask REAL,
                    best_exit_bid_seen REAL,
                    best_exit_bid_seen_at_utc TEXT,
                    gross_pnl_if_exit_now REAL,
                    gross_pnl_at_best_exit REAL,
                    raw_json TEXT,
                    FOREIGN KEY(event_id) REFERENCES events(event_id),
                    FOREIGN KEY(market_id) REFERENCES markets(market_id),
                    FOREIGN KEY(target_market_id) REFERENCES markets(market_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshots_market_time ON snapshots(market_id, captured_at_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_forecast_positions_event ON forecast_positions(event_id)"
            )
            self._run_schema_migrations(conn)

    def _run_schema_migrations(self, conn: sqlite3.Connection) -> None:
        self._ensure_column(conn, "events", "tracking_active", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(conn, "events", "tracking_started_at_utc", "TEXT")
        self._ensure_column(conn, "events", "tracking_stopped_at_utc", "TEXT")
        self._ensure_column(conn, "markets", "initial_no_midpoint", "REAL")
        self._ensure_column(conn, "markets", "initial_no_bid", "REAL")
        self._ensure_column(conn, "markets", "initial_no_ask", "REAL")
        self._ensure_column(conn, "markets", "latest_no_midpoint", "REAL")
        self._ensure_column(conn, "markets", "latest_no_bid", "REAL")
        self._ensure_column(conn, "markets", "latest_no_ask", "REAL")
        self._ensure_column(conn, "snapshots", "no_midpoint", "REAL")
        self._ensure_column(conn, "snapshots", "no_bid", "REAL")
        self._ensure_column(conn, "snapshots", "no_ask", "REAL")

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, column_def: str) -> None:
        existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        if column in existing:
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_def}")

    def run_once(self, stop_tracking_days_after_event: int = DEFAULT_STOP_TRACKING_DAYS_AFTER_EVENT) -> dict[str, int]:
        now = utc_now_iso()
        events = list(self._fetch_active_events())
        highest_events = self._extract_highest_temp_events(events)
        latest_event_date = select_latest_event_date(highest_events)
        if latest_event_date:
            highest_events = [event for event in highest_events if event.event_date_iso == latest_event_date]
        markets = self._extract_markets(highest_events)

        current_event_ids = {e.event_id for e in highest_events}
        current_market_ids = {m.market_id for m in markets}
        event_by_id = {event.event_id: event for event in highest_events}
        new_events = 0
        new_markets = 0
        snapshot_count = 0

        existing_event_ids: set[str] = set()
        existing_market_ids: set[str] = set()
        tracked_event_ids: set[str] = set()
        new_event_ids: set[str] = set()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row

            purged_summary = self._purge_data_outside_event_date(conn, latest_event_date)

            existing_event_ids = {row[0] for row in conn.execute("SELECT event_id FROM events")}
            existing_market_ids = {row[0] for row in conn.execute("SELECT market_id FROM markets")}

            for event in highest_events:
                station_name = None
                resolution_source_url = None
                self._upsert_event(
                    conn=conn,
                    event=event,
                    now=now,
                    unit=infer_event_unit([m for m in markets if m.event_id == event.event_id]),
                    station_name=station_name,
                    resolution_source_url=resolution_source_url,
                )
                if event.event_id not in existing_event_ids:
                    new_events += 1
                    new_event_ids.add(event.event_id)
                    self._activate_event_tracking(conn, event.event_id, now)

            for market in markets:
                is_new = market.market_id not in existing_market_ids
                if is_new:
                    new_markets += 1
                self._upsert_market(conn, market, now, {}, {}, {}, {}, {}, {}, is_new=is_new)

            self._mark_missing_markets_inactive(conn, current_market_ids)
            self._deactivate_stale_event_tracking(
                conn=conn,
                now=now,
                grace_days=max(0, stop_tracking_days_after_event),
            )
            self._deactivate_older_tracked_event_dates(conn=conn, now=now)
            tracked_event_ids = self._load_tracked_event_ids(conn)
            tracked_event_ids = tracked_event_ids.intersection(current_event_ids)
            conn.commit()

        tracked_markets = [market for market in markets if market.event_id in tracked_event_ids]
        yes_token_ids = [m.yes_token_id for m in tracked_markets if m.yes_token_id]
        no_token_ids = [m.no_token_id for m in tracked_markets if m.no_token_id]
        yes_midpoints = self._get_midpoints(yes_token_ids)
        yes_bids = self._get_prices(yes_token_ids, side="BUY")
        yes_asks = self._get_prices(yes_token_ids, side="SELL")
        no_midpoints = self._get_midpoints(no_token_ids)
        no_bids = self._get_prices(no_token_ids, side="BUY")
        no_asks = self._get_prices(no_token_ids, side="SELL")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row

            forecast_pick_event_ids = {
                str(row[0]) for row in conn.execute("SELECT event_id FROM forecast_picks")
            }
            forecast_position_event_ids = {
                str(row[0]) for row in conn.execute("SELECT DISTINCT event_id FROM forecast_positions")
            }

            for market in tracked_markets:
                is_new = market.market_id not in existing_market_ids
                self._upsert_market(
                    conn,
                    market,
                    now,
                    yes_midpoints,
                    yes_bids,
                    yes_asks,
                    no_midpoints,
                    no_bids,
                    no_asks,
                    is_new=is_new,
                )
                self._insert_snapshot(
                    conn,
                    market,
                    now,
                    yes_midpoints,
                    yes_bids,
                    yes_asks,
                    no_midpoints,
                    no_bids,
                    no_asks,
                )
                snapshot_count += 1

            forecast_needed_event_ids = {
                event_id
                for event_id in tracked_event_ids
                if event_id not in forecast_pick_event_ids or event_id not in forecast_position_event_ids
            }

            # Create first-seen forecast state for events that do not have it yet.
            for event_id in sorted(forecast_needed_event_ids):
                if event_id not in tracked_event_ids:
                    continue
                event = event_by_id.get(event_id)
                if event is None:
                    continue
                event_markets = [m for m in tracked_markets if m.event_id == event.event_id]
                if not event_markets:
                    continue
                resolution_meta = self._fetch_resolution_meta(event.url)
                if resolution_meta:
                    conn.execute(
                        """
                        UPDATE events
                        SET station_name = COALESCE(?, station_name),
                            resolution_source_url = COALESCE(?, resolution_source_url)
                        WHERE event_id = ?
                        """,
                        (
                            resolution_meta.get("station_name"),
                            resolution_meta.get("resolution_source_url"),
                            event.event_id,
                        ),
                    )
                forecast = self._get_initial_forecast(event, event_markets)
                if forecast:
                    if event_id not in forecast_pick_event_ids:
                        self._create_forecast_pick(conn, event, event_markets, forecast, now, yes_midpoints, yes_bids, yes_asks)
                    if event_id not in forecast_position_event_ids:
                        self._create_forecast_positions(
                            conn,
                            event,
                            event_markets,
                            forecast,
                            now,
                            yes_midpoints,
                            yes_bids,
                            yes_asks,
                            no_midpoints,
                            no_bids,
                            no_asks,
                        )

            # Update existing forecast picks with current prices.
            self._refresh_forecast_picks(conn, now, yes_midpoints, yes_bids, yes_asks)
            self._refresh_forecast_positions(
                conn,
                now,
                yes_midpoints,
                yes_bids,
                yes_asks,
                no_midpoints,
                no_bids,
                no_asks,
            )
            conn.commit()

        return {
            "events_scanned": len(events),
            "highest_temp_events": len(highest_events),
            "latest_event_date": latest_event_date,
            "markets_discovered": len(markets),
            "markets_tracked": len(tracked_markets),
            "new_events": new_events,
            "new_markets": new_markets,
            "snapshots_inserted": snapshot_count,
            "purged_events": purged_summary["events"],
            "purged_markets": purged_summary["markets"],
            "purged_snapshots": purged_summary["snapshots"],
        }

    def watch_aligned(
        self,
        interval_minutes: int = DEFAULT_POLL_MINUTES,
        start_minute: int = DEFAULT_ALIGN_START_MINUTE,
        stop_tracking_days_after_event: int = DEFAULT_STOP_TRACKING_DAYS_AFTER_EVENT,
    ) -> None:
        interval_minutes = max(1, interval_minutes)
        start_minute = start_minute % interval_minutes
        while True:
            next_run = next_aligned_datetime(
                now=datetime.now(),
                interval_minutes=interval_minutes,
                start_minute=start_minute,
            )
            sleep_seconds = max(0.0, (next_run - datetime.now()).total_seconds())
            logging.info("Next run at %s (sleep %.1fs)", next_run.isoformat(timespec="seconds"), sleep_seconds)
            time.sleep(sleep_seconds)
            started = time.time()
            try:
                summary = self.run_once(stop_tracking_days_after_event=stop_tracking_days_after_event)
                logging.info("Run summary: %s", summary)
            except Exception:
                logging.exception("Run failed")
            elapsed = time.time() - started
            logging.info("Run finished in %.2fs", elapsed)

    def report_picks(self, limit: int = 100) -> list[dict[str, Any]]:
        query = """
            SELECT
                e.title,
                e.event_url,
                fp.created_at_utc,
                fp.forecast_method,
                fp.forecast_source_name,
                fp.forecast_target_market_unit,
                fp.market_unit,
                fp.picked_outcome_label,
                fp.entry_yes_ask,
                fp.latest_yes_bid,
                fp.best_exit_bid_seen,
                fp.gross_pnl_if_exit_now,
                fp.gross_pnl_at_best_exit
            FROM forecast_picks fp
            JOIN events e ON e.event_id = fp.event_id
            ORDER BY fp.created_at_utc DESC
            LIMIT ?
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, (limit,))]

    def report_launch_prices(self, limit_events: int = 3) -> list[dict[str, Any]]:
        query = """
            WITH latest_events AS (
                SELECT
                    e.event_id,
                    e.title,
                    e.city,
                    e.event_date_iso,
                    e.event_url,
                    COALESCE(e.tracking_started_at_utc, e.first_seen_at_utc) AS launch_seen_at_utc
                FROM events e
                ORDER BY launch_seen_at_utc DESC
                LIMIT ?
            )
            SELECT
                le.event_id,
                le.title AS event_title,
                le.city,
                le.event_date_iso,
                le.event_url,
                le.launch_seen_at_utc,
                m.market_id,
                m.outcome_label,
                m.unit,
                m.lower_bound,
                m.upper_bound,
                m.initial_yes_bid,
                m.initial_yes_ask,
                m.initial_no_bid,
                m.initial_no_ask,
                fpos.forecast_stance,
                fp.forecast_method,
                fp.forecast_source_name,
                fp.forecast_target_market_unit,
                fp.market_unit AS forecast_market_unit,
                fp.picked_outcome_label
            FROM latest_events le
            JOIN markets m ON m.event_id = le.event_id
            LEFT JOIN forecast_picks fp ON fp.event_id = le.event_id
            LEFT JOIN forecast_positions fpos ON fpos.market_id = m.market_id
            ORDER BY
                le.launch_seen_at_utc DESC,
                le.event_title ASC,
                CASE WHEN m.lower_bound IS NULL THEN -999999 ELSE m.lower_bound END ASC,
                CASE WHEN m.upper_bound IS NULL THEN 999999 ELSE m.upper_bound END ASC,
                m.outcome_label ASC
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, (limit_events,))]

    def export_csv(self, output_path: str) -> int:
        query = """
            SELECT
                s.captured_at_utc,
                e.title AS event_title,
                e.event_url,
                m.market_id,
                m.market_slug,
                m.outcome_label,
                m.unit,
                m.lower_bound,
                m.upper_bound,
                m.initial_yes_midpoint,
                m.initial_yes_bid,
                m.initial_yes_ask,
                m.initial_no_midpoint,
                m.initial_no_bid,
                m.initial_no_ask,
                s.yes_midpoint,
                s.yes_bid,
                s.yes_ask,
                s.no_midpoint,
                s.no_bid,
                s.no_ask,
                s.spread
            FROM snapshots s
            JOIN markets m ON m.market_id = s.market_id
            JOIN events e ON e.event_id = m.event_id
            ORDER BY s.captured_at_utc ASC, e.title ASC, m.outcome_label ASC
        """
        count = 0
        with sqlite3.connect(self.db_path) as conn, open(output_path, "w", newline="", encoding="utf-8") as f:
            cursor = conn.execute(query)
            writer = csv.writer(f)
            writer.writerow([col[0] for col in cursor.description])
            for row in cursor:
                writer.writerow(row)
                count += 1
        return count

    def export_picks_csv(self, output_path: str) -> int:
        query = """
            SELECT
                e.title,
                e.event_url,
                fp.created_at_utc,
                fp.updated_at_utc,
                fp.forecast_method,
                fp.forecast_source_name,
                fp.forecast_source_url,
                fp.forecast_text,
                fp.forecast_target_raw,
                fp.forecast_target_unit,
                fp.forecast_target_market_unit,
                fp.market_unit,
                fp.latitude,
                fp.longitude,
                fp.timezone_name,
                fp.picked_outcome_label,
                fp.entry_yes_midpoint,
                fp.entry_yes_bid,
                fp.entry_yes_ask,
                fp.latest_yes_midpoint,
                fp.latest_yes_bid,
                fp.latest_yes_ask,
                fp.latest_spread,
                fp.best_mid_seen,
                fp.best_mid_seen_at_utc,
                fp.best_exit_bid_seen,
                fp.best_exit_bid_seen_at_utc,
                fp.gross_pnl_if_exit_now,
                fp.gross_pnl_at_best_exit
            FROM forecast_picks fp
            JOIN events e ON e.event_id = fp.event_id
            ORDER BY fp.created_at_utc ASC
        """
        count = 0
        with sqlite3.connect(self.db_path) as conn, open(output_path, "w", newline="", encoding="utf-8") as f:
            cursor = conn.execute(query)
            writer = csv.writer(f)
            writer.writerow([col[0] for col in cursor.description])
            for row in cursor:
                writer.writerow(row)
                count += 1
        return count

    def export_forecast_positions_csv(self, output_path: str) -> int:
        query = """
            SELECT
                e.title AS event_title,
                e.event_url,
                fp.created_at_utc,
                fp.updated_at_utc,
                fp.forecast_stance,
                fp.target_outcome_label,
                m.market_id,
                m.outcome_label,
                m.unit,
                fp.entry_midpoint,
                fp.entry_bid,
                fp.entry_ask,
                fp.latest_midpoint,
                fp.latest_bid,
                fp.latest_ask,
                fp.best_exit_bid_seen,
                fp.best_exit_bid_seen_at_utc,
                fp.gross_pnl_if_exit_now,
                fp.gross_pnl_at_best_exit
            FROM forecast_positions fp
            JOIN markets m ON m.market_id = fp.market_id
            JOIN events e ON e.event_id = fp.event_id
            ORDER BY fp.created_at_utc ASC, e.title ASC, m.outcome_label ASC
        """
        count = 0
        with sqlite3.connect(self.db_path) as conn, open(output_path, "w", newline="", encoding="utf-8") as f:
            cursor = conn.execute(query)
            writer = csv.writer(f)
            writer.writerow([col[0] for col in cursor.description])
            for row in cursor:
                writer.writerow(row)
                count += 1
        return count

    def _fetch_active_events(self) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            params = {
                "active": "true",
                "closed": "false",
                "limit": self.page_size,
                "offset": offset,
            }
            response = self.session.get(f"{GAMMA_BASE}/events", params=params, timeout=self.timeout_seconds)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict) and "events" in payload:
                events = payload.get("events") or []
                has_more = bool(payload.get("has_more"))
            elif isinstance(payload, list):
                events = payload
                has_more = len(events) >= self.page_size
            else:
                raise RuntimeError(f"Unexpected events payload type: {type(payload)!r}")

            if not events:
                break

            for event in events:
                if isinstance(event, dict):
                    yield event

            if not has_more and len(events) < self.page_size:
                break
            offset += self.page_size

    def _extract_highest_temp_events(self, events: Iterable[dict[str, Any]]) -> list[EventRecord]:
        results: list[EventRecord] = []
        for raw in events:
            title = str(raw.get("title") or raw.get("question") or "").strip()
            if not title.lower().startswith(TITLE_PREFIX):
                continue
            event_id = str(raw.get("id") or raw.get("event_id") or title)
            slug = maybe_str(raw.get("slug"))
            end_date = maybe_str(raw.get("endDate") or raw.get("end_date") or raw.get("endTime") or raw.get("end_time"))
            tags = normalize_tags(raw.get("tags"))
            parsed = parse_highest_temp_title(title)
            url = urljoin(POLYMARKET_BASE, f"/event/{slug}") if slug else None
            results.append(
                EventRecord(
                    event_id=event_id,
                    title=title,
                    slug=slug,
                    end_date=end_date,
                    tags=tags,
                    raw_event=raw,
                    event_date_iso=parsed.get("event_date_iso"),
                    city=parsed.get("city"),
                    url=url,
                )
            )
        return results

    def _extract_markets(self, events: Sequence[EventRecord]) -> list[MarketRecord]:
        extracted: list[MarketRecord] = []
        for event in events:
            markets = event.raw_event.get("markets") or []
            for market in markets:
                if not isinstance(market, dict):
                    continue
                market_id = str(market.get("id") or market.get("conditionId") or market.get("condition_id") or market.get("slug") or "")
                if not market_id:
                    continue
                clob_token_ids = normalize_string_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
                outcome_label = first_non_empty(
                    market.get("question"),
                    market.get("title"),
                    market.get("slug"),
                ) or market_id
                parsed_range = parse_outcome_range(str(outcome_label))
                extracted.append(
                    MarketRecord(
                        event_id=event.event_id,
                        event_title=event.title,
                        event_slug=event.slug,
                        event_end_date=event.end_date,
                        event_date_iso=event.event_date_iso,
                        event_city=event.city,
                        market_id=market_id,
                        market_slug=maybe_str(market.get("slug")),
                        question=str(outcome_label),
                        condition_id=maybe_str(market.get("conditionId") or market.get("condition_id")),
                        yes_token_id=clob_token_ids[0] if len(clob_token_ids) >= 1 else None,
                        no_token_id=clob_token_ids[1] if len(clob_token_ids) >= 2 else None,
                        outcome_label=str(outcome_label),
                        unit=parsed_range.get("unit"),
                        lower_bound=parsed_range.get("lower_bound"),
                        upper_bound=parsed_range.get("upper_bound"),
                        tags=event.tags,
                        raw_event=event.raw_event,
                        raw_market=market,
                    )
                )
        return extracted

    def _get_midpoints(self, token_ids: Sequence[str | None]) -> dict[str, float | None]:
        cleaned = dedupe_preserve_order([t for t in token_ids if t])
        results: dict[str, float | None] = {}
        for chunk in chunked(cleaned, self.batch_size):
            payload = [{"token_id": token_id} for token_id in chunk]
            response = self.session.post(
                f"{CLOB_BASE}/midpoints",
                json=payload,
                timeout=self.timeout_seconds,
            )
            if response.status_code == 400:
                for token_id in chunk:
                    single = self.session.get(
                        f"{CLOB_BASE}/midpoint",
                        params={"token_id": token_id},
                        timeout=self.timeout_seconds,
                    )
                    if single.status_code in (400, 404):
                        results[str(token_id)] = None
                        continue
                    single.raise_for_status()
                    data = single.json()
                    if isinstance(data, dict):
                        results[str(token_id)] = to_float_or_none(
                            data.get("mid_price") or data.get("midpoint") or data.get("price")
                        )
                    else:
                        results[str(token_id)] = None
                continue
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected midpoints payload: {type(data)!r}")
            for token_id, value in data.items():
                results[str(token_id)] = to_float_or_none(value)
        return results

    def _get_prices(self, token_ids: Sequence[str | None], side: str) -> dict[str, float | None]:
        cleaned = dedupe_preserve_order([t for t in token_ids if t])
        results: dict[str, float | None] = {}
        for chunk in chunked(cleaned, self.batch_size):
            payload = [{"token_id": token_id, "side": side} for token_id in chunk]
            response = self.session.post(
                f"{CLOB_BASE}/prices",
                json=payload,
                timeout=self.timeout_seconds,
            )
            if response.status_code == 400:
                for token_id in chunk:
                    single = self.session.get(
                        f"{CLOB_BASE}/price",
                        params={"token_id": token_id, "side": side},
                        timeout=self.timeout_seconds,
                    )
                    if single.status_code in (400, 404):
                        results[str(token_id)] = None
                        continue
                    single.raise_for_status()
                    data = single.json()
                    if isinstance(data, dict):
                        results[str(token_id)] = to_float_or_none(
                            data.get("price") or data.get(side)
                        )
                    else:
                        results[str(token_id)] = None
                continue
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected prices payload: {type(data)!r}")
            for token_id, side_map in data.items():
                if isinstance(side_map, dict):
                    results[str(token_id)] = to_float_or_none(side_map.get(side))
                else:
                    results[str(token_id)] = None
        return results

    def _upsert_event(
        self,
        conn: sqlite3.Connection,
        event: EventRecord,
        now: str,
        unit: str | None,
        station_name: str | None,
        resolution_source_url: str | None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO events (
                event_id, title, slug, event_url, city, event_date_iso, end_date, tags_json,
                unit, station_name, resolution_source_url, tracking_active, tracking_started_at_utc,
                tracking_stopped_at_utc, first_seen_at_utc, last_seen_at_utc, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                title=excluded.title,
                slug=excluded.slug,
                event_url=excluded.event_url,
                city=excluded.city,
                event_date_iso=excluded.event_date_iso,
                end_date=excluded.end_date,
                tags_json=excluded.tags_json,
                unit=COALESCE(events.unit, excluded.unit),
                station_name=COALESCE(events.station_name, excluded.station_name),
                resolution_source_url=COALESCE(events.resolution_source_url, excluded.resolution_source_url),
                last_seen_at_utc=excluded.last_seen_at_utc,
                raw_json=excluded.raw_json
            """,
            (
                event.event_id,
                event.title,
                event.slug,
                event.url,
                event.city,
                event.event_date_iso,
                event.end_date,
                json.dumps(event.tags, ensure_ascii=False),
                unit,
                station_name,
                resolution_source_url,
                now,
                now,
                json.dumps(event.raw_event, ensure_ascii=False),
            ),
        )

    def _upsert_market(
        self,
        conn: sqlite3.Connection,
        market: MarketRecord,
        now: str,
        yes_midpoints: dict[str, float | None],
        yes_bids: dict[str, float | None],
        yes_asks: dict[str, float | None],
        no_midpoints: dict[str, float | None],
        no_bids: dict[str, float | None],
        no_asks: dict[str, float | None],
        is_new: bool,
    ) -> None:
        yes_token_id = market.yes_token_id or ""
        yes_mid = yes_midpoints.get(yes_token_id) if yes_token_id else None
        yes_bid = yes_bids.get(yes_token_id) if yes_token_id else None
        yes_ask = yes_asks.get(yes_token_id) if yes_token_id else None
        no_token_id = market.no_token_id or ""
        no_mid = no_midpoints.get(no_token_id) if no_token_id else None
        no_bid = no_bids.get(no_token_id) if no_token_id else None
        no_ask = no_asks.get(no_token_id) if no_token_id else None
        spread = compute_spread(yes_bid, yes_ask)
        has_yes_quote = int(any(value is not None for value in (yes_mid, yes_bid, yes_ask)))
        has_no_quote = int(any(value is not None for value in (no_mid, no_bid, no_ask)))

        if is_new:
            conn.execute(
                """
                INSERT INTO markets (
                    market_id, event_id, market_slug, question, condition_id, yes_token_id, no_token_id,
                    outcome_label, unit, lower_bound, upper_bound, tags_json,
                    first_seen_at_utc, last_seen_at_utc,
                    initial_yes_midpoint, initial_yes_bid, initial_yes_ask,
                    initial_no_midpoint, initial_no_bid, initial_no_ask,
                    latest_yes_midpoint, latest_yes_bid, latest_yes_ask, latest_spread,
                    latest_no_midpoint, latest_no_bid, latest_no_ask,
                    active, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    market.market_id,
                    market.event_id,
                    market.market_slug,
                    market.question,
                    market.condition_id,
                    market.yes_token_id,
                    market.no_token_id,
                    market.outcome_label,
                    market.unit,
                    market.lower_bound,
                    market.upper_bound,
                    json.dumps(market.tags, ensure_ascii=False),
                    now,
                    now,
                    yes_mid,
                    yes_bid,
                    yes_ask,
                    no_mid,
                    no_bid,
                    no_ask,
                    yes_mid,
                    yes_bid,
                    yes_ask,
                    spread,
                    no_mid,
                    no_bid,
                    no_ask,
                    json.dumps(market.raw_market, ensure_ascii=False),
                ),
            )
            return

        conn.execute(
            """
            UPDATE markets
            SET event_id = ?,
                market_slug = ?,
                question = ?,
                condition_id = ?,
                yes_token_id = ?,
                no_token_id = ?,
                outcome_label = ?,
                unit = ?,
                lower_bound = ?,
                upper_bound = ?,
                tags_json = ?,
                last_seen_at_utc = ?,
                latest_yes_midpoint = CASE WHEN ? = 1 THEN ? ELSE latest_yes_midpoint END,
                latest_yes_bid = CASE WHEN ? = 1 THEN ? ELSE latest_yes_bid END,
                latest_yes_ask = CASE WHEN ? = 1 THEN ? ELSE latest_yes_ask END,
                latest_spread = CASE WHEN ? = 1 THEN ? ELSE latest_spread END,
                latest_no_midpoint = CASE WHEN ? = 1 THEN ? ELSE latest_no_midpoint END,
                latest_no_bid = CASE WHEN ? = 1 THEN ? ELSE latest_no_bid END,
                latest_no_ask = CASE WHEN ? = 1 THEN ? ELSE latest_no_ask END,
                active = 1,
                raw_json = ?
            WHERE market_id = ?
            """,
            (
                market.event_id,
                market.market_slug,
                market.question,
                market.condition_id,
                market.yes_token_id,
                market.no_token_id,
                market.outcome_label,
                market.unit,
                market.lower_bound,
                market.upper_bound,
                json.dumps(market.tags, ensure_ascii=False),
                now,
                has_yes_quote,
                yes_mid,
                has_yes_quote,
                yes_bid,
                has_yes_quote,
                yes_ask,
                has_yes_quote,
                spread,
                has_no_quote,
                no_mid,
                has_no_quote,
                no_bid,
                has_no_quote,
                no_ask,
                json.dumps(market.raw_market, ensure_ascii=False),
                market.market_id,
            ),
        )

    def _insert_snapshot(
        self,
        conn: sqlite3.Connection,
        market: MarketRecord,
        now: str,
        yes_midpoints: dict[str, float | None],
        yes_bids: dict[str, float | None],
        yes_asks: dict[str, float | None],
        no_midpoints: dict[str, float | None],
        no_bids: dict[str, float | None],
        no_asks: dict[str, float | None],
    ) -> None:
        yes_token_id = market.yes_token_id or ""
        yes_mid = yes_midpoints.get(yes_token_id) if yes_token_id else None
        yes_bid = yes_bids.get(yes_token_id) if yes_token_id else None
        yes_ask = yes_asks.get(yes_token_id) if yes_token_id else None
        no_token_id = market.no_token_id or ""
        no_mid = no_midpoints.get(no_token_id) if no_token_id else None
        no_bid = no_bids.get(no_token_id) if no_token_id else None
        no_ask = no_asks.get(no_token_id) if no_token_id else None
        spread = compute_spread(yes_bid, yes_ask)
        conn.execute(
            """
            INSERT OR IGNORE INTO snapshots (
                captured_at_utc, event_id, market_id, yes_token_id,
                yes_midpoint, yes_bid, yes_ask, no_midpoint, no_bid, no_ask, spread
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (now, market.event_id, market.market_id, market.yes_token_id, yes_mid, yes_bid, yes_ask, no_mid, no_bid, no_ask, spread),
        )

    def _purge_data_outside_event_date(
        self,
        conn: sqlite3.Connection,
        keep_event_date_iso: str | None,
    ) -> dict[str, int]:
        summary = {
            "events": 0,
            "markets": 0,
            "snapshots": 0,
            "forecast_picks": 0,
            "forecast_positions": 0,
        }
        if not keep_event_date_iso:
            return summary

        rows = conn.execute(
            """
            SELECT event_id
            FROM events
            WHERE event_date_iso IS NULL OR event_date_iso <> ?
            """,
            (keep_event_date_iso,),
        ).fetchall()
        event_ids = [str(row[0]) for row in rows]
        if not event_ids:
            return summary

        placeholders = ",".join("?" for _ in event_ids)
        summary["snapshots"] = conn.execute(
            f"DELETE FROM snapshots WHERE event_id IN ({placeholders})",
            tuple(event_ids),
        ).rowcount
        summary["forecast_positions"] = conn.execute(
            f"DELETE FROM forecast_positions WHERE event_id IN ({placeholders})",
            tuple(event_ids),
        ).rowcount
        summary["forecast_picks"] = conn.execute(
            f"DELETE FROM forecast_picks WHERE event_id IN ({placeholders})",
            tuple(event_ids),
        ).rowcount
        summary["markets"] = conn.execute(
            f"DELETE FROM markets WHERE event_id IN ({placeholders})",
            tuple(event_ids),
        ).rowcount
        summary["events"] = conn.execute(
            f"DELETE FROM events WHERE event_id IN ({placeholders})",
            tuple(event_ids),
        ).rowcount
        return summary

    def _mark_missing_markets_inactive(self, conn: sqlite3.Connection, current_market_ids: set[str]) -> None:
        if not current_market_ids:
            return
        placeholders = ",".join("?" for _ in current_market_ids)
        conn.execute(f"UPDATE markets SET active = 0 WHERE market_id NOT IN ({placeholders})", tuple(current_market_ids))

    def _activate_event_tracking(self, conn: sqlite3.Connection, event_id: str, now: str) -> None:
        conn.execute(
            """
            UPDATE events
            SET tracking_active = 1,
                tracking_started_at_utc = COALESCE(tracking_started_at_utc, ?),
                tracking_stopped_at_utc = NULL
            WHERE event_id = ?
            """,
            (now, event_id),
        )

    def _deactivate_stale_event_tracking(self, conn: sqlite3.Connection, now: str, grace_days: int) -> None:
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=grace_days)).isoformat()
        conn.execute(
            """
            UPDATE events
            SET tracking_active = 0,
                tracking_stopped_at_utc = COALESCE(tracking_stopped_at_utc, ?)
            WHERE tracking_active = 1
              AND event_date_iso IS NOT NULL
              AND date(event_date_iso) < date(?)
            """,
            (now, cutoff),
        )

    def _load_tracked_event_ids(self, conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute("SELECT event_id FROM events WHERE tracking_active = 1").fetchall()
        return {str(row[0]) for row in rows}

    def _deactivate_older_tracked_event_dates(self, conn: sqlite3.Connection, now: str) -> None:
        rows = conn.execute(
            """
            SELECT event_id, event_date_iso
            FROM events
            WHERE tracking_active = 1
            """
        ).fetchall()
        dated: list[tuple[str, date]] = []
        for row in rows:
            event_id = str(row[0])
            event_date_iso = maybe_str(row[1])
            if not event_date_iso:
                continue
            try:
                parsed = datetime.strptime(event_date_iso, "%Y-%m-%d").date()
            except ValueError:
                continue
            dated.append((event_id, parsed))

        if not dated:
            return

        latest_date = max(item[1] for item in dated)
        to_disable = [event_id for event_id, event_date in dated if event_date < latest_date]
        if not to_disable:
            return
        placeholders = ",".join("?" for _ in to_disable)
        conn.execute(
            f"""
            UPDATE events
            SET tracking_active = 0,
                tracking_stopped_at_utc = COALESCE(tracking_stopped_at_utc, ?)
            WHERE event_id IN ({placeholders})
            """,
            (now, *to_disable),
        )


    def _fetch_resolution_meta(self, event_url: str | None) -> dict[str, str] | None:
        if not event_url:
            return None
        try:
            response = self.session.get(event_url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except Exception as exc:
            logging.warning("Could not fetch event page %s: %s", event_url, exc)
            return None

        text = collapse_space(html.unescape(response.text))
        station_match = re.search(
            r"highest temperature recorded at the (?P<station>.+?) Station .*? available here: (?P<url>https://www\.wunderground\.com/history/daily/[^\s\"'<>]+)",
            text,
            flags=re.IGNORECASE,
        )
        if station_match:
            return {
                "station_name": station_match.group("station").strip(),
                "resolution_source_url": station_match.group("url").strip(),
            }

        url_match = re.search(
            r"(https://www\.wunderground\.com/history/daily/[^\s\"'<>]+)",
            text,
            flags=re.IGNORECASE,
        )
        if url_match:
            return {"station_name": "", "resolution_source_url": url_match.group(1).strip()}
        return None

    def _get_initial_forecast(self, event: EventRecord, event_markets: Sequence[MarketRecord]) -> ForecastInfo | None:
        market_unit = infer_event_unit(event_markets)
        if not market_unit:
            market_unit = "C"
        # Best effort: parse Polymarket event page AI summary first.
        page_forecast = self._parse_forecast_from_polymarket_page(event.url, market_unit)
        if page_forecast:
            return page_forecast

        # Fallback: Open-Meteo based on city/date.
        if not event.city or not event.event_date_iso:
            return None
        return self._fetch_open_meteo_forecast(event.city, event.event_date_iso, market_unit)

    def _parse_forecast_from_polymarket_page(self, event_url: str | None, market_unit: str) -> ForecastInfo | None:
        if not event_url:
            return None
        try:
            response = self.session.get(event_url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except Exception as exc:
            logging.warning("Could not fetch page forecast for %s: %s", event_url, exc)
            return None

        text = collapse_space(html.unescape(response.text))
        patterns = [
            re.compile(
                r"reflecting (?P<source>.+?)'s latest forecast of a (?P<value>-?\d+(?:\.\d+)?)°(?P<unit>[CF]) maximum",
                flags=re.IGNORECASE,
            ),
            re.compile(
                r"latest forecast of a (?P<value>-?\d+(?:\.\d+)?)°(?P<unit>[CF]) maximum",
                flags=re.IGNORECASE,
            ),
        ]
        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            source = match.groupdict().get("source")
            target_value_raw = float(match.group("value"))
            target_unit = match.group("unit").upper()
            target_market_value = convert_temp(target_value_raw, from_unit=target_unit, to_unit=market_unit)
            return ForecastInfo(
                method="polymarket_page_summary",
                source_name=source.strip() if source else "Polymarket page summary",
                source_url=event_url,
                forecast_text=match.group(0),
                target_value_raw=target_value_raw,
                target_unit=target_unit,
                target_value_market_unit=round_half_up(target_market_value),
                market_unit=market_unit,
                city=None,
                event_date_iso=None,
                latitude=None,
                longitude=None,
                timezone_name=None,
                raw_json={"matched_text": match.group(0)},
            )
        return None

    def _fetch_open_meteo_forecast(self, city: str, event_date_iso: str, market_unit: str) -> ForecastInfo | None:
        try:
            geocode_response = self.session.get(
                OPEN_METEO_GEOCODING,
                params={"name": city, "count": 1, "language": "en", "format": "json"},
                timeout=self.timeout_seconds,
            )
            geocode_response.raise_for_status()
            geocode_payload = geocode_response.json()
        except Exception as exc:
            logging.warning("Open-Meteo geocoding failed for %s: %s", city, exc)
            return None

        results = geocode_payload.get("results") if isinstance(geocode_payload, dict) else None
        if not results:
            return None
        location = results[0]
        lat = to_float_or_none(location.get("latitude"))
        lon = to_float_or_none(location.get("longitude"))
        timezone_name = maybe_str(location.get("timezone"))
        if lat is None or lon is None:
            return None

        temp_unit_param = "celsius" if market_unit.upper() == "C" else "fahrenheit"
        forecast_params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max",
            "start_date": event_date_iso,
            "end_date": event_date_iso,
            "temperature_unit": temp_unit_param,
            "timezone": "auto",
        }
        try:
            forecast_response = self.session.get(
                OPEN_METEO_FORECAST,
                params=forecast_params,
                timeout=self.timeout_seconds,
            )
            forecast_response.raise_for_status()
            forecast_payload = forecast_response.json()
        except Exception as exc:
            logging.warning("Open-Meteo forecast failed for %s on %s: %s", city, event_date_iso, exc)
            return None

        daily = forecast_payload.get("daily") if isinstance(forecast_payload, dict) else None
        values = daily.get("temperature_2m_max") if isinstance(daily, dict) else None
        if not values:
            return None

        target_value = to_float_or_none(values[0])
        if target_value is None:
            return None

        return ForecastInfo(
            method="open_meteo",
            source_name="Open-Meteo",
            source_url=build_url(OPEN_METEO_FORECAST, forecast_params),
            forecast_text=f"Open-Meteo daily temperature_2m_max forecast for {city} on {event_date_iso}",
            target_value_raw=target_value,
            target_unit=market_unit,
            target_value_market_unit=round_half_up(target_value),
            market_unit=market_unit,
            city=city,
            event_date_iso=event_date_iso,
            latitude=lat,
            longitude=lon,
            timezone_name=timezone_name,
            raw_json={
                "geocoding": location,
                "forecast": forecast_payload,
            },
        )

    def _create_forecast_pick(
        self,
        conn: sqlite3.Connection,
        event: EventRecord,
        event_markets: Sequence[MarketRecord],
        forecast: ForecastInfo,
        now: str,
        midpoints: dict[str, float | None],
        bids: dict[str, float | None],
        asks: dict[str, float | None],
    ) -> None:
        selected = choose_market_for_target(
            markets=event_markets,
            target_value=forecast.target_value_market_unit,
            market_unit=forecast.market_unit,
        )
        picked_market_id = selected.market_id if selected else None
        picked_outcome_label = selected.outcome_label if selected else None

        entry_mid = entry_bid = entry_ask = None
        if selected and selected.yes_token_id:
            entry_mid = midpoints.get(selected.yes_token_id)
            entry_bid = bids.get(selected.yes_token_id)
            entry_ask = asks.get(selected.yes_token_id)

        gross_now = compute_profit(entry_ask, entry_bid)
        conn.execute(
            """
            INSERT OR REPLACE INTO forecast_picks (
                event_id, created_at_utc, updated_at_utc,
                forecast_method, forecast_source_name, forecast_source_url, forecast_text,
                forecast_target_raw, forecast_target_unit, forecast_target_market_unit, market_unit,
                latitude, longitude, timezone_name,
                picked_market_id, picked_outcome_label,
                entry_yes_midpoint, entry_yes_bid, entry_yes_ask,
                latest_yes_midpoint, latest_yes_bid, latest_yes_ask, latest_spread,
                best_mid_seen, best_mid_seen_at_utc,
                best_exit_bid_seen, best_exit_bid_seen_at_utc,
                gross_pnl_if_exit_now, gross_pnl_at_best_exit,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                now,
                now,
                forecast.method,
                forecast.source_name,
                forecast.source_url,
                forecast.forecast_text,
                forecast.target_value_raw,
                forecast.target_unit,
                forecast.target_value_market_unit,
                forecast.market_unit,
                forecast.latitude,
                forecast.longitude,
                forecast.timezone_name,
                picked_market_id,
                picked_outcome_label,
                entry_mid,
                entry_bid,
                entry_ask,
                entry_mid,
                entry_bid,
                entry_ask,
                compute_spread(entry_bid, entry_ask),
                entry_mid,
                now if entry_mid is not None else None,
                entry_bid,
                now if entry_bid is not None else None,
                gross_now,
                gross_now,
                json.dumps(forecast.raw_json, ensure_ascii=False),
            ),
        )

    def _create_forecast_positions(
        self,
        conn: sqlite3.Connection,
        event: EventRecord,
        event_markets: Sequence[MarketRecord],
        forecast: ForecastInfo,
        now: str,
        yes_midpoints: dict[str, float | None],
        yes_bids: dict[str, float | None],
        yes_asks: dict[str, float | None],
        no_midpoints: dict[str, float | None],
        no_bids: dict[str, float | None],
        no_asks: dict[str, float | None],
    ) -> None:
        target_market = choose_market_for_target(
            markets=event_markets,
            target_value=forecast.target_value_market_unit,
            market_unit=forecast.market_unit,
        )
        target_market_id = target_market.market_id if target_market else None
        target_outcome_label = target_market.outcome_label if target_market else None

        for market in event_markets:
            is_target = target_market_id == market.market_id
            stance = "YES" if is_target else "NO"
            midpoint_map = yes_midpoints if is_target else no_midpoints
            bid_map = yes_bids if is_target else no_bids
            ask_map = yes_asks if is_target else no_asks
            token_id = market.yes_token_id if is_target else market.no_token_id
            entry_mid = midpoint_map.get(token_id) if token_id else None
            entry_bid = bid_map.get(token_id) if token_id else None
            entry_ask = ask_map.get(token_id) if token_id else None
            gross_now = compute_profit(entry_ask, entry_bid)

            conn.execute(
                """
                INSERT OR REPLACE INTO forecast_positions (
                    market_id, event_id, created_at_utc, updated_at_utc,
                    forecast_stance, target_market_id, target_outcome_label,
                    entry_midpoint, entry_bid, entry_ask,
                    latest_midpoint, latest_bid, latest_ask,
                    best_exit_bid_seen, best_exit_bid_seen_at_utc,
                    gross_pnl_if_exit_now, gross_pnl_at_best_exit,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    market.market_id,
                    event.event_id,
                    now,
                    now,
                    stance,
                    target_market_id,
                    target_outcome_label,
                    entry_mid,
                    entry_bid,
                    entry_ask,
                    entry_mid,
                    entry_bid,
                    entry_ask,
                    entry_bid,
                    now if entry_bid is not None else None,
                    gross_now,
                    gross_now,
                    json.dumps(
                        {
                            "forecast_target_market_unit": forecast.target_value_market_unit,
                            "forecast_market_unit": forecast.market_unit,
                            "event_city": event.city,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )

    def _refresh_forecast_picks(
        self,
        conn: sqlite3.Connection,
        now: str,
        midpoints: dict[str, float | None],
        bids: dict[str, float | None],
        asks: dict[str, float | None],
    ) -> None:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT fp.event_id, fp.picked_market_id, fp.entry_yes_ask,
                   fp.best_mid_seen, fp.best_exit_bid_seen,
                   m.yes_token_id
            FROM forecast_picks fp
            LEFT JOIN markets m ON m.market_id = fp.picked_market_id
            """
        ).fetchall()

        for row in rows:
            token_id = row["yes_token_id"]
            if not token_id:
                continue
            if token_id not in midpoints and token_id not in bids and token_id not in asks:
                continue
            current_mid = midpoints.get(token_id)
            current_bid = bids.get(token_id)
            current_ask = asks.get(token_id)
            current_spread = compute_spread(current_bid, current_ask)

            best_mid_seen = row["best_mid_seen"]
            best_mid_seen_at = None
            if current_mid is not None and (best_mid_seen is None or current_mid > best_mid_seen):
                best_mid_seen = current_mid
                best_mid_seen_at = now

            best_exit_bid_seen = row["best_exit_bid_seen"]
            best_exit_bid_seen_at = None
            if current_bid is not None and (best_exit_bid_seen is None or current_bid > best_exit_bid_seen):
                best_exit_bid_seen = current_bid
                best_exit_bid_seen_at = now

            entry_ask = row["entry_yes_ask"]
            gross_now = compute_profit(entry_ask, current_bid)
            gross_best = compute_profit(entry_ask, best_exit_bid_seen)

            conn.execute(
                """
                UPDATE forecast_picks
                SET updated_at_utc = ?,
                    latest_yes_midpoint = ?,
                    latest_yes_bid = ?,
                    latest_yes_ask = ?,
                    latest_spread = ?,
                    best_mid_seen = ?,
                    best_mid_seen_at_utc = COALESCE(?, best_mid_seen_at_utc),
                    best_exit_bid_seen = ?,
                    best_exit_bid_seen_at_utc = COALESCE(?, best_exit_bid_seen_at_utc),
                    gross_pnl_if_exit_now = ?,
                    gross_pnl_at_best_exit = ?
                WHERE event_id = ?
                """,
                (
                    now,
                    current_mid,
                    current_bid,
                    current_ask,
                    current_spread,
                    best_mid_seen,
                    best_mid_seen_at,
                    best_exit_bid_seen,
                    best_exit_bid_seen_at,
                    gross_now,
                    gross_best,
                    row["event_id"],
                ),
            )

    def _refresh_forecast_positions(
        self,
        conn: sqlite3.Connection,
        now: str,
        yes_midpoints: dict[str, float | None],
        yes_bids: dict[str, float | None],
        yes_asks: dict[str, float | None],
        no_midpoints: dict[str, float | None],
        no_bids: dict[str, float | None],
        no_asks: dict[str, float | None],
    ) -> None:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT fp.market_id, fp.event_id, fp.forecast_stance, fp.entry_ask,
                   fp.best_exit_bid_seen, m.yes_token_id, m.no_token_id
            FROM forecast_positions fp
            JOIN markets m ON m.market_id = fp.market_id
            """
        ).fetchall()

        for row in rows:
            use_yes = row["forecast_stance"] == "YES"
            token_id = row["yes_token_id"] if use_yes else row["no_token_id"]
            if not token_id:
                continue

            midpoint_map = yes_midpoints if use_yes else no_midpoints
            bid_map = yes_bids if use_yes else no_bids
            ask_map = yes_asks if use_yes else no_asks
            if token_id not in midpoint_map and token_id not in bid_map and token_id not in ask_map:
                continue

            current_mid = midpoint_map.get(token_id)
            current_bid = bid_map.get(token_id)
            current_ask = ask_map.get(token_id)
            best_exit_bid_seen = row["best_exit_bid_seen"]
            best_exit_bid_seen_at = None
            if current_bid is not None and (best_exit_bid_seen is None or current_bid > best_exit_bid_seen):
                best_exit_bid_seen = current_bid
                best_exit_bid_seen_at = now

            entry_ask = row["entry_ask"]
            gross_now = compute_profit(entry_ask, current_bid)
            gross_best = compute_profit(entry_ask, best_exit_bid_seen)

            conn.execute(
                """
                UPDATE forecast_positions
                SET updated_at_utc = ?,
                    latest_midpoint = ?,
                    latest_bid = ?,
                    latest_ask = ?,
                    best_exit_bid_seen = ?,
                    best_exit_bid_seen_at_utc = COALESCE(?, best_exit_bid_seen_at_utc),
                    gross_pnl_if_exit_now = ?,
                    gross_pnl_at_best_exit = ?
                WHERE market_id = ?
                """,
                (
                    now,
                    current_mid,
                    current_bid,
                    current_ask,
                    best_exit_bid_seen,
                    best_exit_bid_seen_at,
                    gross_now,
                    gross_best,
                    row["market_id"],
                ),
            )


def parse_highest_temp_title(title: str) -> dict[str, str | None]:
    match = re.match(r"^Highest temperature in (?P<city>.+?) on (?P<when>.+?)\?$", title.strip(), flags=re.IGNORECASE)
    if not match:
        return {"city": None, "event_date_iso": None}
    city = match.group("city").strip()
    when_text = match.group("when").strip()
    event_date_iso = parse_flexible_date(when_text)
    return {"city": city, "event_date_iso": event_date_iso}


def select_latest_event_date(events: Sequence[EventRecord]) -> str | None:
    dated = sorted({event.event_date_iso for event in events if event.event_date_iso})
    if not dated:
        return None
    return dated[-1]


def parse_flexible_date(text: str) -> str | None:
    candidates = [
        ("%B %d, %Y", False),
        ("%b %d, %Y", False),
        ("%B %d", True),
        ("%b %d", True),
    ]
    for fmt, needs_year in candidates:
        try:
            parsed = datetime.strptime(text, fmt)
            if needs_year:
                parsed = parsed.replace(year=datetime.now().year)
            return parsed.date().isoformat()
        except ValueError:
            continue
    return None


def parse_outcome_range(label: str) -> dict[str, float | None]:
    text = collapse_space(label.strip())

    patterns = [
        (
            re.compile(
                r"(?:between\s+)?(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])",
                flags=re.IGNORECASE,
            ),
            "range",
        ),
        (
            re.compile(
                r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s+or\s+below",
                flags=re.IGNORECASE,
            ),
            "low",
        ),
        (
            re.compile(
                r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s+or\s+(?:higher|above)",
                flags=re.IGNORECASE,
            ),
            "high",
        ),
        (
            re.compile(
                r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])",
                flags=re.IGNORECASE,
            ),
            "exact",
        ),
    ]

    for pattern, kind in patterns:
        match = pattern.search(text)
        if not match:
            continue
        if kind == "range":
            low = float(match.group(1))
            high = float(match.group(2))
            unit = match.group(3).upper()
            return {"lower_bound": min(low, high), "upper_bound": max(low, high), "unit": unit}
        if kind == "low":
            value = float(match.group(1))
            unit = match.group(2).upper()
            return {"lower_bound": None, "upper_bound": value, "unit": unit}
        if kind == "high":
            value = float(match.group(1))
            unit = match.group(2).upper()
            return {"lower_bound": value, "upper_bound": None, "unit": unit}
        value = float(match.group(1))
        unit = match.group(2).upper()
        return {"lower_bound": value, "upper_bound": value, "unit": unit}

    return {"lower_bound": None, "upper_bound": None, "unit": infer_unit_from_text(text)}


def choose_market_for_target(
    markets: Sequence[MarketRecord],
    target_value: float | None,
    market_unit: str | None,
) -> MarketRecord | None:
    if target_value is None:
        return None
    market_unit = (market_unit or "").upper() or None
    candidates: list[tuple[float, float, MarketRecord]] = []
    for market in markets:
        if market_unit and market.unit and market.unit.upper() != market_unit:
            continue
        if contains_value(market.lower_bound, market.upper_bound, target_value):
            width = interval_width(market.lower_bound, market.upper_bound)
            midpoint_distance = abs(interval_midpoint(market.lower_bound, market.upper_bound, target_value) - target_value)
            candidates.append((width, midpoint_distance, market))
    if candidates:
        candidates.sort(key=lambda item: (item[0], item[1], item[2].outcome_label))
        return candidates[0][2]

    # Fallback: nearest interval midpoint or boundary.
    fallback: list[tuple[float, float, MarketRecord]] = []
    for market in markets:
        if market_unit and market.unit and market.unit.upper() != market_unit:
            continue
        representative = interval_midpoint(market.lower_bound, market.upper_bound, target_value)
        distance = abs(representative - target_value)
        width = interval_width(market.lower_bound, market.upper_bound)
        fallback.append((distance, width, market))
    if not fallback:
        return None
    fallback.sort(key=lambda item: (item[0], item[1], item[2].outcome_label))
    return fallback[0][2]


def contains_value(lower: float | None, upper: float | None, target: float) -> bool:
    if lower is not None and target < lower:
        return False
    if upper is not None and target > upper:
        return False
    return True


def interval_midpoint(lower: float | None, upper: float | None, target: float) -> float:
    if lower is not None and upper is not None:
        return (lower + upper) / 2.0
    if lower is None and upper is not None:
        return upper
    if lower is not None and upper is None:
        return lower
    return target


def interval_width(lower: float | None, upper: float | None) -> float:
    if lower is None or upper is None:
        return math.inf
    return abs(upper - lower)


def infer_event_unit(markets: Sequence[MarketRecord]) -> str | None:
    for market in markets:
        if market.unit:
            return market.unit
    return None


def infer_unit_from_text(text: str) -> str | None:
    m = re.search(r"([CF])\b", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


def convert_temp(value: float, from_unit: str, to_unit: str) -> float:
    f = from_unit.upper()
    t = to_unit.upper()
    if f == t:
        return value
    if f == "C" and t == "F":
        return value * 9.0 / 5.0 + 32.0
    if f == "F" and t == "C":
        return (value - 32.0) * 5.0 / 9.0
    raise ValueError(f"Unsupported conversion {from_unit} -> {to_unit}")


def compute_profit(entry_ask: float | None, exit_bid: float | None) -> float | None:
    if entry_ask is None or exit_bid is None:
        return None
    return exit_bid - entry_ask


def normalize_tags(tags: Any) -> list[str]:
    if tags is None:
        return []
    if isinstance(tags, str):
        parsed = maybe_json_loads(tags)
        if isinstance(parsed, list):
            return normalize_tags(parsed)
        return [tags]
    if isinstance(tags, list):
        out: list[str] = []
        for item in tags:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                for key in ("name", "label", "title", "slug"):
                    if item.get(key):
                        out.append(str(item[key]))
                        break
        return dedupe_preserve_order(out)
    return []


def normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and str(v) != ""]
    if isinstance(value, str):
        parsed = maybe_json_loads(value)
        if isinstance(parsed, list):
            return [str(v) for v in parsed if v is not None and str(v) != ""]
        return [part.strip() for part in value.split(",") if part.strip()]
    return [str(value)]


def maybe_json_loads(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return value


def to_float_or_none(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def maybe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def first_non_empty(*values: Any) -> str | None:
    for value in values:
        text = maybe_str(value)
        if text:
            return text
    return None


def dedupe_preserve_order(items: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def chunked(items: Sequence[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield list(items[i:i + size])


def compute_spread(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return ask - bid


def format_price(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.4f}"


def collapse_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def build_url(base: str, params: dict[str, Any]) -> str:
    return f"{base}?{urlencode(params, doseq=True)}"


def round_half_up(value: float) -> float:
    if value >= 0:
        return math.floor(value + 0.5)
    return math.ceil(value - 0.5)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def next_aligned_datetime(now: datetime, interval_minutes: int, start_minute: int) -> datetime:
    start_minute = start_minute % interval_minutes
    candidate = now.replace(second=0, microsecond=0)
    if now.second > 0 or now.microsecond > 0:
        candidate += timedelta(minutes=1)

    while candidate.minute % interval_minutes != start_minute:
        candidate += timedelta(minutes=1)
    return candidate


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DEFAULT_DB, help=f"SQLite database path (default: {DEFAULT_DB})")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Polymarket events page size")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="CLOB batch size")
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument(
        "--stop-tracking-days-after-event",
        type=int,
        default=DEFAULT_STOP_TRACKING_DAYS_AFTER_EVENT,
        help="Keep tracking until this many days after event_date_iso (default: 1)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run-once", help="Discover new highest-temperature events and snapshot tracked ones")

    watch = sub.add_parser("watch-aligned", help="Run forever aligned to minutes 1,6,11,... by default")
    watch.add_argument("--interval-minutes", type=int, default=DEFAULT_POLL_MINUTES, help="Polling interval")
    watch.add_argument("--start-minute", type=int, default=DEFAULT_ALIGN_START_MINUTE, help="Starting minute offset")

    export = sub.add_parser("export-csv", help="Export all market snapshots to CSV")
    export.add_argument("--out", required=True, help="Output CSV path")

    export_picks = sub.add_parser("export-picks-csv", help="Export event-level forecast picks to CSV")
    export_picks.add_argument("--out", required=True, help="Output CSV path")

    export_forecast_positions = sub.add_parser(
        "export-forecast-positions-csv",
        help="Export per-market forecast YES/NO positions to CSV",
    )
    export_forecast_positions.add_argument("--out", required=True, help="Output CSV path")

    report = sub.add_parser("report-picks", help="Print latest tracked forecast picks")
    report.add_argument("--limit", type=int, default=50, help="Max rows")

    report_launch = sub.add_parser(
        "report-launch",
        help="Print launch prices per outcome (YES/NO) for latest events",
    )
    report_launch.add_argument("--limit-events", type=int, default=3, help="How many events")
    return parser


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.verbose)

    tracker = HighestTemperatureTracker(
        db_path=args.db,
        page_size=args.page_size,
        batch_size=args.batch_size,
        timeout_seconds=args.timeout,
    )

    if args.command == "run-once":
        summary = tracker.run_once(stop_tracking_days_after_event=args.stop_tracking_days_after_event)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0

    if args.command == "watch-aligned":
        tracker.watch_aligned(
            interval_minutes=args.interval_minutes,
            start_minute=args.start_minute,
            stop_tracking_days_after_event=args.stop_tracking_days_after_event,
        )
        return 0

    if args.command == "export-csv":
        rows = tracker.export_csv(args.out)
        print(f"Exported {rows} snapshot rows to {args.out}")
        return 0

    if args.command == "export-picks-csv":
        rows = tracker.export_picks_csv(args.out)
        print(f"Exported {rows} forecast-pick rows to {args.out}")
        return 0

    if args.command == "export-forecast-positions-csv":
        rows = tracker.export_forecast_positions_csv(args.out)
        print(f"Exported {rows} forecast-position rows to {args.out}")
        return 0

    if args.command == "report-picks":
        rows = tracker.report_picks(limit=args.limit)
        for row in rows:
            print(json.dumps(row, ensure_ascii=False))
        return 0

    if args.command == "report-launch":
        rows = tracker.report_launch_prices(limit_events=args.limit_events)
        if not rows:
            print("No launch rows found")
            return 0

        current_event_id = None
        for row in rows:
            event_id = row.get("event_id")
            if event_id != current_event_id:
                current_event_id = event_id
                print("")
                print(f"Event: {row.get('event_title')}")
                print(f"Launch seen: {row.get('launch_seen_at_utc')}")
                forecast_target = row.get("forecast_target_market_unit")
                forecast_unit = row.get("forecast_market_unit")
                forecast_source = row.get("forecast_source_name")
                if forecast_target is not None:
                    print(
                        "Forecast max: "
                        f"{forecast_target}{forecast_unit or ''} "
                        f"(source: {forecast_source or 'unknown'})"
                    )
                else:
                    print("Forecast max: NA")
                picked = row.get("picked_outcome_label")
                if picked:
                    print(f"Picked outcome: {picked}")
                print("Outcomes at launch (SI / NO):")

            print(
                f"- [{row.get('forecast_stance') or '-'}] {row.get('outcome_label')} | "
                f"SI bid {format_price(row.get('initial_yes_bid'))} ask {format_price(row.get('initial_yes_ask'))} | "
                f"NO bid {format_price(row.get('initial_no_bid'))} ask {format_price(row.get('initial_no_ask'))}"
            )
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
