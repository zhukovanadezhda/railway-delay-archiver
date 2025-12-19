import argparse
import csv
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

from fetch_stop_areas import setup_logging


def init_db(db_path: Path) -> sqlite3.Connection:
    """
    Connect to an existing SQLite database and ensure that
    the 'trains' table exists with the expected schema.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trains (
        train_instance_id TEXT PRIMARY KEY,
        vehicle_journey_id TEXT NOT NULL,
        service_date DATE NOT NULL,

        stop_area_id TEXT NOT NULL,
        train_type TEXT,

        scheduled_time TIMESTAMP NOT NULL,
        realtime_time TIMESTAMP,

        delay_sec INTEGER,
        possibly_cancelled BOOLEAN,

        nb_observations INTEGER,
        seen_base_schedule BOOLEAN,
        seen_realtime BOOLEAN,

        last_seen_delta_sec INTEGER,
        last_poll_timestamp TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_trains_last_poll
    ON trains(last_poll_timestamp)
    """)

    conn.commit()
    return conn


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None

    for fmt in ("%Y%m%dT%H%M%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass

    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def upsert_train(cur, row: dict):
    poll_ts = parse_dt(row.get("poll_timestamp"))
    sched_ts = parse_dt(row.get("scheduled_time"))
    rt_ts = parse_dt(row.get("realtime_time"))

    if poll_ts is None or sched_ts is None:
        return

    service_date = sched_ts.date().isoformat()
    train_instance_id = f"{row['vehicle_journey_id']}_{service_date}"

    seen_base = int(row.get("data_freshness") == "base_schedule")
    seen_rt = int(row.get("data_freshness") == "realtime")

    last_seen_delta = (
        int((rt_ts - poll_ts).total_seconds())
        if rt_ts else None
    )

    delay_sec = (
        int(row["delay_sec"])
        if row.get("delay_sec") not in (None, "")
        else None
    )

    # Cancellation logic handled later → default False
    possibly_cancelled = 0

    cur.execute("""
    INSERT INTO trains VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    ON CONFLICT(train_instance_id) DO UPDATE SET
        realtime_time = excluded.realtime_time,
        delay_sec = excluded.delay_sec,
        last_seen_delta_sec = excluded.last_seen_delta_sec,
        last_poll_timestamp = excluded.last_poll_timestamp,

        nb_observations = trains.nb_observations + 1,
        seen_base_schedule = MAX(trains.seen_base_schedule, excluded.seen_base_schedule),
        seen_realtime = MAX(trains.seen_realtime, excluded.seen_realtime),
        train_type = COALESCE(trains.train_type, excluded.train_type),
        possibly_cancelled = trains.possibly_cancelled

    WHERE excluded.last_poll_timestamp > trains.last_poll_timestamp
    """, (
        train_instance_id,
        row["vehicle_journey_id"],
        service_date,
        row["stop_area_id"],
        row.get("train_type"),
        sched_ts.isoformat(),
        rt_ts.isoformat() if rt_ts else None,
        delay_sec,
        possibly_cancelled,
        1,
        seen_base,
        seen_rt,
        last_seen_delta,
        poll_ts.isoformat()
    ))


def aggregate_raw_files(raw_dir: Path, db_path: Path, commit_every: int):
    conn = init_db(db_path)
    cur = conn.cursor()

    raw_files = sorted(
        p for p in raw_dir.glob("raw_*.csv")
        if not p.name.endswith("_parsed.csv")
    )

    logging.info("Found %d raw files", len(raw_files))
    processed = 0

    for raw_file in raw_files:
        logging.info("Processing %s", raw_file.name)

        with raw_file.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in tqdm(reader, desc=raw_file.name, unit="rows"):
                try:
                    upsert_train(cur, row)
                    processed += 1

                    if processed % commit_every == 0:
                        conn.commit()

                except Exception as e:
                    logging.warning("Skipping row: %s", e)

        conn.commit()
        raw_file.rename(raw_file.with_name(raw_file.stem + "_parsed.csv"))

    conn.commit()
    conn.close()
    logging.info("Aggregation finished (%d rows processed)", processed)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Populate the trains table from raw SNCF observations"
    )
    parser.add_argument(
        "--raw-dir",
        required=True,
        type=Path,
        help="Directory containing raw CSV files"
    )
    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="Path to EXISTING SQLite database (railway.db)"
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=1000,
        help="Commit every N rows"
    )
    parser.add_argument(
        "--log-level",
        default="INFO"
    )

    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)

    aggregate_raw_files(
        raw_dir=args.raw_dir,
        db_path=args.db,
        commit_every=args.commit_every
    )


if __name__ == "__main__":
    main()