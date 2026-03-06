"""
YHZ Hourly Climate Data Pipeline
=================================
Extracts hourly climate observations for Halifax Stanfield International Airport
(station 8202251) from the MSC GeoMet API, transforms the data, and loads it
into the yhz_db SQLite database.

ETL Stages:
    1. Extract   - Fetch all hourly records for the latest available full date
    2. Transform - Clean data: map fields, replace NULLs with 0, add insert_time
    3. Load      - Write rows to SQLite in a single atomic transaction
    4. Validate  - Confirm rows landed correctly in the database
    5. Print     - Display all loaded rows in a formatted table
    6. Audit     - Record run metadata to pipeline_runs table

Usage:
    python pipeline.py
    python pipeline.py --db /path/to/yhz_db.sqlite
    python pipeline.py --log-level DEBUG

Environment Variables:
    DB_PATH    Path to the SQLite database (default: yhz_db.sqlite)
    LOG_LEVEL  Logging verbosity: DEBUG, INFO, WARNING, ERROR (default: INFO)
"""

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone

import requests

# =============================================================================
# CONFIGURATION
# =============================================================================

# API endpoint for hourly climate data
API_BASE_URL = "https://api.weather.gc.ca/collections/climate-hourly/items"

# Halifax Stanfield International Airport station ID
CLIMATE_IDENTIFIER = "8202251"

# Target table in yhz_db.sqlite
TARGET_TABLE = "hiaa_geomet_hourly"

# Audit table — records metadata about every pipeline run
AUDIT_TABLE = "pipeline_runs"

# Database path — overridden by environment variable or CLI argument
DEFAULT_DB_PATH = os.environ.get("DB_PATH", "yhz_db.sqlite")

# Log level — overridden by environment variable or CLI argument
DEFAULT_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# -----------------------------------------------------------------------------
# Exact column names and order matching the hiaa_geomet_hourly table.
# Verified against the actual yhz_db.sqlite schema.
# -----------------------------------------------------------------------------
TABLE_COLUMNS = [
    "climate_identifier",
    "local_date",
    "temp",
    "dew_point_temp",
    "humidex",
    "precip_amount",
    "relative_humidity",
    "station_pressure",
    "visibility",
    "weather_eng_desc",
    "windchill",
    "wind_direction",
    "wind_speed",
    "insert_time",
]

# -----------------------------------------------------------------------------
# The API returns fields in UPPERCASE. This maps each DB column name
# to its corresponding API property name for the transform step.
# -----------------------------------------------------------------------------
API_FIELD_MAP = {
    "climate_identifier": "CLIMATE_IDENTIFIER",
    "local_date":         "LOCAL_DATE",
    "temp":               "TEMP",
    "dew_point_temp":     "DEW_POINT_TEMP",
    "humidex":            "HUMIDEX",
    "precip_amount":      "PRECIP_AMOUNT",
    "relative_humidity":  "RELATIVE_HUMIDITY",
    "station_pressure":   "STATION_PRESSURE",
    "visibility":         "VISIBILITY",
    "weather_eng_desc":   "WEATHER_ENG_DESC",
    "windchill":          "WINDCHILL",
    "wind_direction":     "WIND_DIRECTION",
    "wind_speed":         "WIND_SPEED",
}

# -----------------------------------------------------------------------------
# Numeric columns where NULL/None must be replaced with 0
# as per the pipeline requirements.
# -----------------------------------------------------------------------------
NUMERIC_COLUMNS = {
    "temp",
    "dew_point_temp",
    "humidex",
    "precip_amount",
    "relative_humidity",
    "station_pressure",
    "visibility",
    "windchill",
    "wind_direction",
    "wind_speed",
}

# -----------------------------------------------------------------------------
# Column display config for the printed results table.
# Each entry: (header label, width)
# -----------------------------------------------------------------------------
DISPLAY_COLUMNS = [
    ("climate_identifier", "STATION ID",    12),
    ("local_date",         "DATE/TIME",     22),
    ("temp",               "TEMP °C",        8),
    ("dew_point_temp",     "DEW PT °C",     10),
    ("humidex",            "HUMIDEX",        8),
    ("precip_amount",      "PRECIP mm",     10),
    ("relative_humidity",  "HUMIDITY %",    11),
    ("station_pressure",   "PRESSURE kPa",  13),
    ("visibility",         "VIS km",         7),
    ("weather_eng_desc",   "WEATHER",       16),
    ("windchill",          "WINDCHILL",     10),
    ("wind_direction",     "WIND DIR",       9),
    ("wind_speed",         "WIND SPD",       9),
    ("insert_time",        "INSERT TIME",   22),
]


# =============================================================================
# LOGGING
# =============================================================================

def configure_logging(level: str) -> logging.Logger:
    """
    Set up structured logging to both stdout and pipeline.log.

    Writing to a file means logs are saved for review after the run,
    which is essential for diagnosing issues in production.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[
            logging.StreamHandler(sys.stdout),    # Print to screen
            logging.FileHandler("pipeline.log"),  # Save to log file
        ],
    )
    return logging.getLogger("yhz.pipeline")


# =============================================================================
# EXTRACT
# =============================================================================

def fetch_latest_full_date(logger: logging.Logger) -> str:
    """
    Determine the latest available full date from the API.

    Queries the most recent records for station 8202251 sorted descending
    and returns the most recent date that is not today.

    Today is excluded because the current day may be incomplete —
    loading partial data would give an inaccurate picture of the day.

    Returns:
        A date string in YYYY-MM-DD format e.g. '2026-03-05'
    """
    logger.info(
        "Determining latest available full date for station %s",
        CLIMATE_IDENTIFIER
    )

    params = {
        "CLIMATE_IDENTIFIER": CLIMATE_IDENTIFIER,
        "sortby": "-LOCAL_DATE",
        "limit": 25,
        "f": "json",
    }

    response = _api_get(API_BASE_URL, params, logger)
    features = response.get("features", [])

    if not features:
        raise ValueError("No data returned from API. Cannot determine latest date.")

    # Collect distinct past dates — skip today to avoid loading a partial day
    today_utc = datetime.now(timezone.utc).date().isoformat()
    dates_seen = []

    for feature in features:
        local_date = feature.get("properties", {}).get("LOCAL_DATE", "")
        date_only = local_date[:10] if local_date else ""
        if date_only and date_only not in dates_seen and date_only < today_utc:
            dates_seen.append(date_only)

    if not dates_seen:
        raise ValueError(
            "No complete past dates found. All records may belong to today."
        )

    latest_date = dates_seen[0]
    logger.info("Latest available full date: %s", latest_date)
    return latest_date


def extract(target_date: str, logger: logging.Logger) -> list[dict]:
    """
    Download all hourly records for station 8202251 on the given date.

    Paginates through the API until all records are retrieved.
    A full day has 24 records (one per hour).

    Pagination ensures the pipeline handles any volume of data —
    not just the current 24 records per day.

    Args:
        target_date: Date string in YYYY-MM-DD format

    Returns:
        List of raw GeoJSON feature dictionaries from the API
    """
    logger.info("Extracting hourly data for date: %s", target_date)

    all_records = []
    offset = 0
    limit = 100  # Max records per API page

    while True:
        params = {
            "CLIMATE_IDENTIFIER": CLIMATE_IDENTIFIER,
            "LOCAL_DATE": target_date,
            "limit": limit,
            "offset": offset,
            "f": "json",
        }

        response = _api_get(API_BASE_URL, params, logger)
        features = response.get("features", [])
        logger.debug("Fetched %d records (offset=%d)", len(features), offset)
        all_records.extend(features)

        # Stop when fewer records than the page size are returned
        if len(features) < limit:
            break
        offset += limit

    logger.info("Extracted %d total records for %s", len(all_records), target_date)
    return all_records


def _api_get(url: str, params: dict, logger: logging.Logger) -> dict:
    """
    Make a GET request to the API with error handling.

    Handles three failure modes:
    - Timeout:    API took longer than 30 seconds to respond
    - HTTP Error: API returned a 4xx or 5xx status code
    - Connection: No internet or DNS failure

    Args:
        url: API endpoint URL
        params: Query parameters

    Returns:
        Parsed JSON response as a dictionary
    """
    logger.debug("GET %s | params=%s", url, params)
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        raise RuntimeError(f"API request timed out after 30s: {url}")
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(
            f"API returned HTTP {exc.response.status_code}: {url}"
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"API request failed: {exc}") from exc

    return response.json()


# =============================================================================
# TRANSFORM
# =============================================================================

def transform(features: list[dict], logger: logging.Logger) -> list[dict]:
    """
    Transform raw API records into rows matching the database schema.

    For each record this function:
      1. Selects only the fields present in the target table
      2. Renames fields from uppercase API names to lowercase DB column names
      3. Replaces NULL/None values in numeric columns with 0
      4. Adds insert_time as the current UTC timestamp

    Args:
        features: Raw GeoJSON feature dictionaries from the API

    Returns:
        List of clean row dictionaries ready to insert into the database
    """
    insert_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(
        "Transforming %d records (insert_time=%s)", len(features), insert_time
    )

    rows = []
    for feature in features:
        props = feature.get("properties", {})
        row = {}

        for col in TABLE_COLUMNS:
            # insert_time is generated by the pipeline, not sourced from API
            if col == "insert_time":
                row[col] = insert_time
                continue

            # Translate DB column name to API field name
            api_field = API_FIELD_MAP[col]
            value = props.get(api_field)

            # Replace NULL with 0 for numeric columns (pipeline requirement)
            if col in NUMERIC_COLUMNS and value is None:
                value = 0

            row[col] = value

        rows.append(row)

    logger.info("Transform complete. %d rows ready for load.", len(rows))
    return rows


# =============================================================================
# LOAD
# =============================================================================

def load(rows: list[dict], db_path: str, logger: logging.Logger) -> None:
    """
    Insert transformed rows into the SQLite database table.

    Uses INSERT OR REPLACE so the pipeline is idempotent — re-running
    for the same date updates existing rows rather than creating duplicates.
    This means the pipeline can be safely re-run after any failure.

    All rows are written in a single transaction. If any row fails,
    the entire transaction is rolled back — the table is never left
    in a partial or inconsistent state.

    Args:
        rows:    List of clean row dictionaries to insert
        db_path: Path to the SQLite database file
    """
    if not rows:
        logger.warning("No rows to load. Skipping database write.")
        return

    logger.info(
        "Loading %d rows into %s -> %s", len(rows), db_path, TARGET_TABLE
    )

    col_list = ", ".join(TABLE_COLUMNS)
    placeholders = ", ".join(["?" for _ in TABLE_COLUMNS])
    sql = (
        f"INSERT OR REPLACE INTO {TARGET_TABLE} "
        f"({col_list}) VALUES ({placeholders})"
    )

    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            values = [[row[col] for col in TABLE_COLUMNS] for row in rows]
            cursor.executemany(sql, values)
            conn.commit()
            logger.info("Successfully committed %d rows.", len(rows))
        except sqlite3.Error as exc:
            conn.rollback()
            raise RuntimeError(
                f"Database write failed — transaction rolled back: {exc}"
            ) from exc
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            f"Cannot open database at '{db_path}': {exc}"
        ) from exc


# =============================================================================
# VALIDATE
# =============================================================================

def validate(
    target_date: str, db_path: str, logger: logging.Logger
) -> None:
    """
    Verify the loaded data looks correct after each run.

    Confirms at least one row exists for the target date.
    Catches silent failures immediately — better to know now
    than to discover missing data days later.

    Args:
        target_date: The date that was loaded, in YYYY-MM-DD format
        db_path:     Path to the SQLite database file

    Raises:
        RuntimeError if no rows are found for the target date
    """
    logger.info("Validating loaded data for %s...", target_date)

    conn = sqlite3.connect(db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM hiaa_geomet_hourly WHERE local_date LIKE ?",
            (f"{target_date}%",),
        ).fetchone()[0]

        if count == 0:
            raise RuntimeError(
                f"Validation failed: no rows found for {target_date}"
            )

        logger.info(
            "Validation passed: %d rows found for %s.", count, target_date
        )
    finally:
        conn.close()


# =============================================================================
# PRINT RESULTS
# =============================================================================

def print_results(
    target_date: str, db_path: str, logger: logging.Logger
) -> None:
    """
    Print all loaded rows in a clean formatted table to the console.

    Displays every row loaded for the target date with aligned columns,
    borders, and a summary footer so data can be visually confirmed
    after every run.

    Args:
        target_date: The date that was loaded, in YYYY-MM-DD format
        db_path:     Path to the SQLite database file
    """
    logger.info("Printing loaded data for %s...", target_date)

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM hiaa_geomet_hourly "
            "WHERE local_date LIKE ? ORDER BY local_date",
            (f"{target_date}%",),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print("\n  No data found for", target_date)
        return

    # Build column index map so we pull values in DISPLAY_COLUMNS order
    col_index = {col: i for i, col in enumerate(TABLE_COLUMNS)}

    # Calculate total table width
    total_width = sum(w for _, _, w in DISPLAY_COLUMNS) + (3 * len(DISPLAY_COLUMNS)) + 1

    # ── Header ────────────────────────────────────────────────────────────
    print()
    print("┌" + "─" * (total_width - 2) + "┐")
    title = f"  YHZ CLIMATE DATA  ·  Station {CLIMATE_IDENTIFIER}  ·  {target_date}  ·  {len(rows)} rows loaded"
    print("│" + title.center(total_width - 2) + "│")
    print("├" + "─" * (total_width - 2) + "┤")

    # ── Column headers ────────────────────────────────────────────────────
    header_cells = ""
    for _, label, width in DISPLAY_COLUMNS:
        header_cells += "│ " + label[:width].center(width) + " "
    header_cells += "│"
    print(header_cells)

    # ── Separator ─────────────────────────────────────────────────────────
    sep = ""
    for _, _, width in DISPLAY_COLUMNS:
        sep += "├" + "─" * (width + 2)
    sep += "┤"
    print(sep)

    # ── Data rows ─────────────────────────────────────────────────────────
    for i, row in enumerate(rows):
        # Alternate row shading marker (just spacing, works in any terminal)
        line = ""
        for col, _, width in DISPLAY_COLUMNS:
            val = row[col_index[col]]
            val_str = str(val) if val is not None else "—"
            # Right-align numbers, left-align text
            if col in NUMERIC_COLUMNS:
                cell = val_str[:width].rjust(width)
            else:
                cell = val_str[:width].ljust(width)
            line += "│ " + cell + " "
        line += "│"
        print(line)

    # ── Footer ────────────────────────────────────────────────────────────
    print("└" + "─" * (total_width - 2) + "┘")
    print(f"  ✓ {len(rows)} rows  ·  Date: {target_date}  ·  Loaded at: {rows[0][col_index['insert_time']]}")
    print()


# =============================================================================
# AUDIT
# =============================================================================

def record_audit(
    db_path: str,
    target_date: str,
    rows_loaded: int,
    status: str,
    error_message: str,
    logger: logging.Logger,
) -> None:
    """
    Record metadata about this pipeline run in a pipeline_runs audit table.

    Creates a permanent history of every execution showing when it ran,
    how many rows were loaded, and whether it succeeded or failed.

    Useful for answering operational questions like:
      - Did the pipeline run last night?
      - How many rows were loaded?
      - What went wrong on a specific date?

    The audit table is created automatically on first run.
    Audit failures are logged as warnings but never crash the pipeline.

    Args:
        db_path:       Path to the SQLite database file
        target_date:   The date that was loaded
        rows_loaded:   Number of rows written to the database
        status:        'SUCCESS' or 'FAILED'
        error_message: Error details if FAILED, empty string otherwise
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                run_time      TEXT,
                target_date   TEXT,
                rows_loaded   INTEGER,
                status        TEXT,
                error_message TEXT
            )
            """
        )
        conn.execute(
            f"INSERT INTO {AUDIT_TABLE} VALUES (?, ?, ?, ?, ?)",
            (
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                target_date,
                rows_loaded,
                status,
                error_message,
            ),
        )
        conn.commit()
        logger.info(
            "Run recorded in %s (status=%s).", AUDIT_TABLE, status
        )
    except sqlite3.Error as exc:
        # Audit failure should never crash the main pipeline
        logger.warning("Could not write to audit table: %s", exc)
    finally:
        conn.close()


# =============================================================================
# ENTRY POINT
# =============================================================================

def run(db_path: str, logger: logging.Logger) -> None:
    """
    Execute the full ETL pipeline:
        1. Find the latest available full date
        2. Extract all hourly records for that date
        3. Transform records to match the database schema
        4. Load rows into the database
        5. Validate the load was successful
        6. Print loaded rows in a formatted table
        7. Record the run in the audit table
    """
    logger.info("=== YHZ Climate Pipeline START ===")

    target_date = "unknown"
    rows_loaded = 0

    try:
        # Step 1 - Find the target date
        target_date = fetch_latest_full_date(logger)

        # Step 2 - Extract
        features = extract(target_date, logger)

        # Step 3 - Transform
        rows = transform(features, logger)

        # Step 4 - Load
        load(rows, db_path, logger)
        rows_loaded = len(rows)

        # Step 5 - Validate
        validate(target_date, db_path, logger)

        # Step 6 - Print results table
        print_results(target_date, db_path, logger)

        # Step 7 - Audit success
        record_audit(
            db_path, target_date, rows_loaded, "SUCCESS", "", logger
        )

    except Exception as exc:
        # Record failure in audit table before re-raising
        record_audit(
            db_path, target_date, rows_loaded, "FAILED", str(exc), logger
        )
        raise

    logger.info("=== YHZ Climate Pipeline COMPLETE ===")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="YHZ Hourly Climate ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py
  python pipeline.py --db /data/yhz_db.sqlite
  python pipeline.py --log-level DEBUG
        """,
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help="Path to SQLite database (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default=DEFAULT_LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: %(default)s)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    log = configure_logging(args.log_level)
    try:
        run(db_path=args.db, logger=log)
    except Exception as exc:
        log.error("Pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)
