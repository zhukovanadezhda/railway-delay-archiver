#!/usr/bin/env bash
set -euo pipefail

# ============================================
# Railway Delay Archiver — Main Pipeline
# ============================================

# -------------------------------
# CONFIG
# -------------------------------

DB_PATH="data/railway.db"
RAW_DIR="data/raw"
ENV_FILE=".env"
LOG_LEVEL="INFO"

LOOP_SLEEP_SECONDS=3600     # 1 hour between passes
RETRY_DELAY=60              # 1 min between retries
MAX_RETRIES=3
SLEEP_24H=86400             # 24 hours

CREATE_CSV=false
if [[ "${1:-}" == "create_csv" ]]; then
    CREATE_CSV=true
fi

mkdir -p data
mkdir -p "$RAW_DIR"

echo "========================================"
echo " Railway Delay Archiver - Main Pipeline "
echo "========================================"

# ============================================
# Helper: run command with retry on API quota
# ============================================

run_with_retry () {
    local CMD="$1"
    local ATTEMPT=1

    while true; do
        echo "[RUN] Attempt $ATTEMPT/$MAX_RETRIES"
        eval "$CMD"
        EXIT_CODE=$?

        if [[ $EXIT_CODE -eq 0 ]]; then
            echo "[RUN] Success"
            return 0
        fi

        if [[ $EXIT_CODE -eq 42 ]]; then
            echo "[WARN] API quota exceeded (HTTP 429)"

            if [[ $ATTEMPT -ge $MAX_RETRIES ]]; then
                echo "[FATAL] Quota exhausted after $MAX_RETRIES attempts"
                echo "[SLEEP] Sleeping for 24 hours..."
                sleep "$SLEEP_24H"
                ATTEMPT=1
            else
                echo "[RETRY] Sleeping $RETRY_DELAY seconds before retry"
                sleep "$RETRY_DELAY"
                ATTEMPT=$((ATTEMPT + 1))
            fi
        else
            echo "[ERROR] Command failed with exit code $EXIT_CODE"
            exit $EXIT_CODE
        fi
    done
}

# ============================================
# STEP 0 — Ensure database exists
# ============================================

if [[ ! -f "$DB_PATH" ]]; then
    echo "[INIT] Creating SQLite database at $DB_PATH"
    sqlite3 "$DB_PATH" ""
else
    echo "[INIT] Database already exists: $DB_PATH"
fi

# ============================================
# STEP 0 — Ensure stations table exists
# ============================================

STATIONS_EXIST=$(sqlite3 "$DB_PATH" "
    SELECT name FROM sqlite_master
    WHERE type='table' AND name='stations';
")

if [[ -z "$STATIONS_EXIST" ]]; then
    echo "[STEP 0] Stations table not found — fetching stop areas"

    run_with_retry "python scripts/fetch_stop_areas.py \
        --env \"$ENV_FILE\" \
        --db \"$DB_PATH\" \
        --page-size 200 \
        --sleep 0.2 \
        --log-level \"$LOG_LEVEL\""

else
    echo "[STEP 0] Stations table already exists — skipping fetch"
fi

# ============================================
# MAIN LOOP
# ============================================

PASS=0

while true; do
    PASS=$((PASS + 1))

    echo ""
    echo "========================================"
    echo "[LOOP] Pipeline pass #$PASS"
    echo "Timestamp (UTC): $(date -u)"
    echo "========================================"

    # ----------------------------------------
    # STEP 1 — Scrape realtime departures
    # ----------------------------------------

    echo "[STEP 1] Scraping realtime departures"

    run_with_retry "python scripts/scrape_departure_delays.py \
        --db \"$DB_PATH\" \
        --env \"$ENV_FILE\" \
        --output-dir \"$RAW_DIR\" \
        --log-level \"$LOG_LEVEL\""

    # ----------------------------------------
    # STEP 2 — Aggregate raw logs
    # ----------------------------------------

    echo "[STEP 2] Aggregating raw observations into trains table"

    python scripts/aggregate_realtime_logs.py \
        --raw-dir "$RAW_DIR" \
        --db "$DB_PATH" \
        --commit-every 1000 \
        --log-level "$LOG_LEVEL"

    # ----------------------------------------
    # STEP 3 — Enrich with calendar & weather
    # ----------------------------------------

    echo "[STEP 3] Enriching with calendar and weather"

    python scripts/enrich_with_weather_calendar.py \
        --db "$DB_PATH" \
        --log-level "$LOG_LEVEL"

    # ----------------------------------------
    # OPTIONAL — Export unified CSV
    # ----------------------------------------

    if [[ "$CREATE_CSV" == true ]]; then
        echo "[EXPORT] Creating unified CSV dataset"

        python scripts/export_unified_dataset.py \
            --db "$DB_PATH" \
            --output data/train_delays.csv \
            --log-level "$LOG_LEVEL"
    fi

    echo "[LOOP] Pass #$PASS completed"
    echo "[LOOP] Sleeping for $LOOP_SLEEP_SECONDS seconds"
    sleep "$LOOP_SLEEP_SECONDS"
done
