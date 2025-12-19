import argparse
import logging
import sqlite3
from datetime import timedelta

import pandas as pd
import numpy as np
from tqdm import tqdm
import holidays

from meteostat import Point, Hourly

from fetch_stop_areas import setup_logging


def floor_to_hour(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.replace(minute=0, second=0, microsecond=0)


def season_from_month(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def sql_safe(value):
    """
    Convert pandas / numpy missing values to None for SQLite.
    """
    if value is None:
        return None

    # pandas NA
    if value is pd.NA:
        return None

    # numpy NaN
    if isinstance(value, float) and np.isnan(value):
        return None

    return value


def create_calendar_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calendar (
            date DATE PRIMARY KEY,
            weekday INTEGER,
            is_weekend BOOLEAN,
            is_holiday_fr BOOLEAN,
            month INTEGER,
            season TEXT
        )
    """)
    conn.commit()


def populate_calendar(conn: sqlite3.Connection):
    logging.info("Populating calendar table")

    df = pd.read_sql("""
        SELECT DISTINCT DATE(
            COALESCE(realtime_time, scheduled_time)
        ) AS date
        FROM trains
        WHERE scheduled_time IS NOT NULL
    """, conn)

    fr_holidays = holidays.country_holidays("FR")

    rows = []
    for d in df["date"].dropna().unique():
        d = pd.to_datetime(d).date()
        rows.append((
            d.isoformat(),
            d.weekday(),
            d.weekday() >= 5,
            d in fr_holidays,
            d.month,
            season_from_month(d.month)
        ))

    conn.executemany("""
        INSERT OR IGNORE INTO calendar
        (date, weekday, is_weekend, is_holiday_fr, month, season)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    logging.info("Calendar rows inserted: %d", len(rows))


def create_weather_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            stop_area_id TEXT,
            weather_hour TIMESTAMP,

            temperature REAL,
            precipitation REAL,
            snowfall REAL,
            wind_speed REAL,
            wind_gust REAL,
            visibility REAL,
            weather_code INTEGER,

            PRIMARY KEY (stop_area_id, weather_hour)
        )
    """)
    conn.commit()


def fetch_weather(lat, lon, hour_ts):
    start = hour_ts.to_pydatetime()
    end = (hour_ts + timedelta(hours=1)).to_pydatetime()

    data = Hourly(Point(lat, lon), start, end).fetch()
    if data.empty:
        return None

    r = data.iloc[0]
    return (
        r.get("temp"),
        r.get("prcp"),
        r.get("snow"),
        r.get("wspd"),
        r.get("wpgt"),
        r.get("vis"),
        r.get("coco")
    )


def populate_weather(conn: sqlite3.Connection):
    logging.info("Populating weather table")

    df = pd.read_sql("""
        SELECT DISTINCT
            t.stop_area_id,
            s.latitude,
            s.longitude,
            DATETIME(
                STRFTIME('%Y-%m-%d %H:00:00',
                    COALESCE(t.realtime_time, t.scheduled_time)
                )
            ) AS weather_hour
        FROM trains t
        JOIN stations s USING (stop_area_id)
        WHERE s.latitude IS NOT NULL
          AND s.longitude IS NOT NULL
    """, conn)

    weather_cache = {}
    rows = []

    for _, r in tqdm(df.iterrows(), total=len(df), desc="Weather", unit="key"):
        key = (r.latitude, r.longitude, r.weather_hour)

        if key not in weather_cache:
            try:
                weather_cache[key] = fetch_weather(
                    r.latitude,
                    r.longitude,
                    pd.to_datetime(r.weather_hour)
                )
            except Exception as e:
                logging.warning("Weather fetch failed %s: %s", key, e)
                weather_cache[key] = None

        if weather_cache[key] is None:
            continue

        rows.append((
            sql_safe(r.stop_area_id),
            sql_safe(r.weather_hour),
            *[sql_safe(v) for v in weather_cache[key]]
        ))

    conn.executemany("""
        INSERT OR IGNORE INTO weather (
            stop_area_id, weather_hour,
            temperature, precipitation, snowfall,
            wind_speed, wind_gust, visibility, weather_code
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()
    logging.info("Weather rows inserted: %d", len(rows))

def parse_args():
    parser = argparse.ArgumentParser(
        description="Populate calendar and weather tables from trains & stations"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="SQLite database path with trains and stations"
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

    conn = sqlite3.connect(args.db)

    create_calendar_table(conn)
    populate_calendar(conn)

    create_weather_table(conn)
    populate_weather(conn)

    conn.close()
    logging.info("Enrichment completed successfully")


if __name__ == "__main__":
    main()
