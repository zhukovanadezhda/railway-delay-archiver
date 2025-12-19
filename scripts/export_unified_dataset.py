import argparse
import csv
import logging
import sqlite3
from pathlib import Path

from fetch_stop_areas import setup_logging


def export_unified_dataset(db_path: Path, output_csv: Path):
    logging.info("Connecting to database: %s", db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    logging.info("Running unified export query")

    query = """
    SELECT
        -- TRAIN CORE
        t.train_instance_id,
        t.vehicle_journey_id,
        t.service_date,
        t.stop_area_id,
        t.train_type,

        t.scheduled_time,
        t.realtime_time,
        t.delay_sec,
        t.possibly_cancelled,

        t.nb_observations,
        t.seen_base_schedule,
        t.seen_realtime,
        t.last_seen_delta_sec,
        t.last_poll_timestamp,

        -- STATION
        s.name AS station_name,
        s.latitude,
        s.longitude,
        s.timezone,
        s.administrative_region,

        -- CALENDAR
        c.weekday,
        c.is_weekend,
        c.is_holiday_fr,
        c.month,
        c.season,

        -- WEATHER
        w.temperature,
        w.precipitation,
        w.snowfall,
        w.wind_speed,
        w.wind_gust,
        w.visibility,
        w.weather_code

    FROM trains t

    LEFT JOIN stations s
        ON t.stop_area_id = s.stop_area_id

    LEFT JOIN calendar c
        ON t.service_date = c.date

    LEFT JOIN weather w
        ON w.stop_area_id = t.stop_area_id
       AND w.weather_hour = strftime(
            '%Y-%m-%d %H:00:00',
            COALESCE(t.realtime_time, t.scheduled_time)
       )

    ORDER BY t.service_date, t.scheduled_time
    """

    cur.execute(query)

    rows = cur.fetchall()
    if not rows:
        logging.warning("No rows returned by query")

    logging.info("Writing CSV to %s (%d rows)", output_csv, len(rows))

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(rows[0].keys())
        for row in rows:
            writer.writerow(row)

    conn.close()
    logging.info("Export completed successfully")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export unified train delay dataset from SQLite database"
    )

    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="Path to railway SQLite database"
    )

    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Output CSV file"
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )

    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)

    export_unified_dataset(
        db_path=args.db,
        output_csv=args.output
    )


if __name__ == "__main__":
    main()
