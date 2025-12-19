import argparse
import logging
import os
import sys
import time
import sqlite3
from typing import Dict, List

import requests
from dotenv import load_dotenv
from tqdm import tqdm


BASE_URL = "https://api.sncf.com/v1/coverage/sncf/stop_areas"
DEFAULT_PAGE_SIZE = 200
DEFAULT_SLEEP = 0.2


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def load_sncf_token(env_path: str) -> str:
    load_dotenv(env_path)
    token = os.getenv("SNCF_TOKEN")
    if not token:
        logging.error("SNCF_TOKEN not found in environment")
        sys.exit(1)
    return token


def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS stations (
        stop_area_id TEXT PRIMARY KEY,
        name TEXT,
        latitude REAL,
        longitude REAL,
        timezone TEXT,
        administrative_region TEXT
    )
    """)

    conn.commit()
    return conn


def insert_station(conn: sqlite3.Connection, row: List):
    conn.execute("""
    INSERT OR IGNORE INTO stations (
        stop_area_id,
        name,
        latitude,
        longitude,
        timezone,
        administrative_region
    ) VALUES (?, ?, ?, ?, ?, ?)
    """, row)


def fetch_stop_areas_page(
    token: str,
    start_page: int,
    page_size: int,
) -> List[Dict]:
    params = {
        "start_page": start_page,
        "count": page_size
    }

    response = requests.get(
        BASE_URL,
        params=params,
        auth=(token, ""),
        timeout=30
    )
    response.raise_for_status()

    data = response.json()
    return data.get("stop_areas", [])


def parse_stop_area(sa: Dict) -> List:
    coord = sa.get("coord", {})
    admin_regions = sa.get("administrative_regions", [])

    return [
        sa.get("id"),
        sa.get("name"),
        coord.get("lat"),
        coord.get("lon"),
        sa.get("timezone"),
        admin_regions[0].get("name") if admin_regions else None
    ]


def fetch_all_stop_areas(
    token: str,
    db_path: str,
    page_size: int,
    sleep_seconds: float,
) -> None:
    logging.info("Initializing database")
    conn = init_db(db_path)

    start_page = 0
    total_rows = 0

    logging.info("Starting stop_areas retrieval")

    with tqdm(desc="Pages fetched", unit="page") as pbar:
        while True:
            stop_areas = fetch_stop_areas_page(
                token=token,
                start_page=start_page,
                page_size=page_size
            )

            if not stop_areas:
                break

            for sa in stop_areas:
                insert_station(conn, parse_stop_area(sa))

            conn.commit()
            total_rows += len(stop_areas)
            start_page += 1
            pbar.update(1)

            time.sleep(sleep_seconds)

    conn.close()
    logging.info("Finished fetching stop_areas")
    logging.info("Total stop_areas processed: %d", total_rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch all SNCF stop areas and store them in SQLite"
    )
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to .env file containing SNCF_TOKEN"
    )
    parser.add_argument(
        "--db",
        default="data/railway.db",
        help="SQLite database file"
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Number of stop areas per API page"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Sleep time between requests (seconds)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    token = load_sncf_token(args.env)

    fetch_all_stop_areas(
        token=token,
        db_path=args.db,
        page_size=args.page_size,
        sleep_seconds=args.sleep,
    )


if __name__ == "__main__":
    main()
