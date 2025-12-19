import argparse
import csv
import logging
import os
import random
import sqlite3
import time
from datetime import datetime
from typing import Dict, List

import requests
from dotenv import load_dotenv
from tqdm import tqdm

from fetch_stop_areas import BASE_URL, setup_logging, load_sncf_token


MAX_RETRIES = 5
MAX_BACKOFF_SECONDS = 30
REQUEST_TIMEOUT = 30


def load_stop_areas_from_db(db_path: str) -> List[str]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT stop_area_id FROM stations ORDER BY stop_area_id")
    stop_areas = [row[0] for row in cur.fetchall()]

    conn.close()

    if not stop_areas:
        raise RuntimeError("No stations found in table 'stations'")

    return stop_areas


def fetch_departures(token: str, stop_area_id: str) -> List[Dict]:
    url = f"{BASE_URL}/{stop_area_id}/departures"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                params={"data_freshness": "realtime"},
                auth=(token, ""),
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                return response.json().get("departures", [])

            if response.status_code == 429 or 500 <= response.status_code < 600:
                wait = min(MAX_BACKOFF_SECONDS, 2**attempt + random.random())
                logging.warning(
                    "HTTP %d for %s (attempt %d/%d), sleeping %.1fs",
                    response.status_code,
                    stop_area_id,
                    attempt,
                    MAX_RETRIES,
                    wait
                )
                time.sleep(wait)
                continue

            logging.error("Non-retryable HTTP %d for %s", response.status_code, stop_area_id)
            return []

        except requests.RequestException as e:
            wait = min(MAX_BACKOFF_SECONDS, 2**attempt + random.random())
            logging.warning(
                "Network error for %s (attempt %d/%d): %s — sleeping %.1fs",
                stop_area_id,
                attempt,
                MAX_RETRIES,
                str(e),
                wait
            )
            time.sleep(wait)

    logging.error("Giving up on %s after %d retries", stop_area_id, MAX_RETRIES)
    return []


def extract_rows(departures: List[Dict], stop_area_id: str) -> List[List]:
    rows = []
    poll_ts = datetime.utcnow().isoformat()

    for dep in departures:
        sdt = dep.get("stop_date_time", {})
        base = sdt.get("base_departure_date_time")
        rt = sdt.get("departure_date_time")

        if not base or not rt:
            continue

        try:
            t_base = datetime.strptime(base, "%Y%m%dT%H%M%S")
            t_rt = datetime.strptime(rt, "%Y%m%dT%H%M%S")
        except ValueError:
            continue

        delay = int((t_rt - t_base).total_seconds())

        vehicle_journey_id = next(
            (l["id"] for l in dep.get("links", []) if l.get("type") == "vehicle_journey"),
            None,
        )

        line = dep.get("route", {}).get("line", {})
        train_type = line.get("commercial_mode", {}).get("name")

        is_terminus = any(l.get("type") == "terminus" for l in dep.get("links", []))

        disruption_id = next(
            (l["id"] for l in dep.get("links", []) if l.get("type") == "disruption"),
            None,
        )

        rows.append([
            poll_ts,
            stop_area_id,
            vehicle_journey_id,
            base,
            rt,
            delay,
            sdt.get("data_freshness"),
            train_type,
            is_terminus,
            disruption_id
        ])

    return rows


def scrape(token: str, db_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    stop_areas = load_stop_areas_from_db(db_path)
    logging.info("Loaded %d stop areas from DB (%s)", len(stop_areas), db_path)

    try:
        pass_ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
        output_csv = os.path.join(output_dir, f"raw_{pass_ts}.csv")

        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "poll_timestamp",
                "stop_area_id",
                "vehicle_journey_id",
                "scheduled_time",
                "realtime_time",
                "delay_sec",
                "data_freshness",
                "train_type",
                "is_terminus",
                "disruption_id"
            ])

            with tqdm(stop_areas, unit="station") as pbar:
                for stop_area_id in pbar:
                    departures = fetch_departures(token, stop_area_id)
                    rows = extract_rows(departures, stop_area_id)
                    if rows:
                        writer.writerows(rows)
                        f.flush()


    except KeyboardInterrupt:
        logging.info("Scraper stopped by user")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Robust SNCF realtime departures scraper with rotating logs"
    )
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to .env containing SNCF_TOKEN"
        )
    parser.add_argument(
        "--db",
        default="data/database/stations.db",
        help="SQLite DB containing table stations"
        )
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Directory to store raw CSV outputs"
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

    load_dotenv(args.env, override=True)
    token = load_sncf_token(args.env)

    scrape(
        token=token,
        db_path=args.db,
        output_dir=args.output_dir
    )


if __name__ == "__main__":
    main()
