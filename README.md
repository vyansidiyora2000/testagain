# YHZ Hourly Climate Data Pipeline

A production-ready ETL pipeline that extracts hourly climate observations for **Halifax Stanfield International Airport** (station `8202251`) from the MSC GeoMet API, transforms the data, and loads it into a local SQLite database on a daily automated schedule.

---

## Overview

| Property     | Value |
|--------------|-------|
| Data Source  | [MSC GeoMet API](https://api.weather.gc.ca/collections/climate-hourly/items) |
| Station      | Halifax Stanfield International Airport (`8202251`) |
| Granularity  | Hourly observations (24 records per day) |
| Target Table | `hiaa_geomet_hourly` in `yhz_db.sqlite` |
| Language     | Python 3.11+ |
| Schedule     | Daily at 06:00 UTC via cron |

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                         pipeline.py                             │
│                                                                 │
│  ┌──────────┐  ┌───────────┐  ┌────────┐  ┌──────────┐        │
│  │ EXTRACT  │─▶│ TRANSFORM │─▶│  LOAD  │─▶│ VALIDATE │        │
│  │          │  │           │  │        │  │          │        │
│  │ GeoMet   │  │ Map fields│  │ SQLite │  │ Confirm  │        │
│  │ REST API │  │ NULL → 0  │  │ INSERT │  │ rows     │        │
│  │ Paginated│  │ Timestamp │  │   OR   │  │ landed   │        │
│  │          │  │           │  │REPLACE │  │          │        │
│  └──────────┘  └───────────┘  └────────┘  └────┬─────┘        │
│                                                 │              │
│                                      ┌──────────▼──────────┐   │
│                                      │    PRINT RESULTS    │   │
│                                      │  Formatted table of │   │
│                                      │  all 24 rows        │   │
│                                      └──────────┬──────────┘   │
│                                                 │              │
│                                           ┌─────▼─────┐        │
│                                           │   AUDIT   │        │
│                                           │ Log run   │        │
│                                           │ metadata  │        │
│                                           └───────────┘        │
└─────────────────────────────────────────────────────────────────┘
                ▲
        cron — 06:00 UTC daily
```

The pipeline runs 7 stages on every execution:

**1. Extract** — Automatically discovers the latest complete date in the API. Today is skipped to avoid loading a partial day. Paginates through all 24 hourly records for that date.

**2. Transform** — Maps uppercase API field names to lowercase database column names. Replaces `NULL` in numeric columns with `0`. Stamps each row with the current UTC time as `insert_time`.

**3. Load** — Writes all rows in a single atomic transaction using `INSERT OR REPLACE`. If any row fails the whole batch rolls back — the table is never left in a partial state.

**4. Validate** — Confirms rows were actually written to the database after every load. Catches silent failures immediately.

**5. Print Results** — Displays all loaded rows in a clean formatted table in the console so data can be visually confirmed.

**6. Audit** — Records run metadata (timestamp, rows loaded, status, error) to a `pipeline_runs` table. Creates a full history of every execution.

---

## Project Structure

```
Halifax-Pipeline/
├── pipeline.py        # Main ETL pipeline
├── requirements.txt   # Pinned Python dependencies
├── install.sh         # One-command server setup
├── setup_cron.sh      # Registers daily cron job
├── yhz_db.sqlite      # SQLite database
├── .gitignore         # Git ignore rules
└── README.md          # This file
```

---

## Requirements

- **OS:** Linux (Ubuntu 20.04+ recommended)
- **Python:** 3.11 or higher
- **Internet:** Required to reach `api.weather.gc.ca`
- **SQLite:** Included in Python standard library — no installation needed

### Install Python if not already present

```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3-pip
```

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/vyansidiyora2000/Halifax-Pipeline.git
cd Halifax-Pipeline

# 2. Install dependencies
bash install.sh

# 3. Run the pipeline
.venv/bin/python pipeline.py
```

Three commands from a clean server to a running pipeline.

---

## Running the Pipeline

### Basic run
```bash
.venv/bin/python pipeline.py
```

### With debug logging
```bash
.venv/bin/python pipeline.py --log-level DEBUG
```

### With a custom database path
```bash
.venv/bin/python pipeline.py --db /path/to/yhz_db.sqlite
```

### Expected output
```
2026-03-06T06:00:01Z [INFO] yhz.pipeline - === YHZ Climate Pipeline START ===
2026-03-06T06:00:01Z [INFO] yhz.pipeline - Determining latest available full date for station 8202251
2026-03-06T06:00:02Z [INFO] yhz.pipeline - Latest available full date: 2026-03-05
2026-03-06T06:00:02Z [INFO] yhz.pipeline - Extracting hourly data for date: 2026-03-05
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Extracted 24 total records for 2026-03-05
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Transforming 24 records (insert_time=2026-03-06T06:00:03Z)
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Transform complete. 24 rows ready for load.
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Loading 24 rows into yhz_db.sqlite -> hiaa_geomet_hourly
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Successfully committed 24 rows.
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Validating loaded data for 2026-03-05...
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Validation passed: 24 rows found for 2026-03-05.
2026-03-06T06:00:03Z [INFO] yhz.pipeline - Printing loaded data for 2026-03-05...

┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│          YHZ CLIMATE DATA  ·  Station 8202251  ·  2026-03-05  ·  24 rows loaded             │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ STATION ID   │ DATE/TIME              │ TEMP °C │ DEW PT °C │ ...                           │
├──────────────┼────────────────────────┼─────────┼───────────┼─ ...                          │
│ 8202251      │ 2026-03-05T00:00:00    │    -2.1 │      -5.0 │ ...                           │
│ 8202251      │ 2026-03-05T01:00:00    │    -1.8 │      -4.8 │ ...                           │
│ ...          │ ...                    │     ... │       ... │ ...                           │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
  ✓ 24 rows  ·  Date: 2026-03-05  ·  Loaded at: 2026-03-06T06:00:03Z

2026-03-06T06:00:03Z [INFO] yhz.pipeline - Run recorded in pipeline_runs (status=SUCCESS).
2026-03-06T06:00:03Z [INFO] yhz.pipeline - === YHZ Climate Pipeline COMPLETE ===
```

### Exit codes

| Code | Meaning |
|------|---------|
| `0`  | Pipeline completed successfully |
| `1`  | Pipeline failed — check log output for details |

---

## Scheduling (Production)

Register the pipeline to run automatically every day at **06:00 UTC**:

```bash
bash setup_cron.sh
```

Verify it was registered:
```bash
crontab -l
```

Monitor live logs:
```bash
tail -f pipeline.log
```

---

## Configuration

| CLI Argument  | Environment Variable | Default         | Description |
|---------------|----------------------|-----------------|-------------|
| `--db`        | `DB_PATH`            | `yhz_db.sqlite` | Path to SQLite database |
| `--log-level` | `LOG_LEVEL`          | `INFO`          | Logging verbosity |

Using environment variables:
```bash
export DB_PATH=/data/yhz_db.sqlite
export LOG_LEVEL=DEBUG
.venv/bin/python pipeline.py
```

---

## Database Schema

### `hiaa_geomet_hourly` — Climate data table

| Column               | Type    | Notes                               |
|----------------------|---------|-------------------------------------|
| `climate_identifier` | TEXT    | Station ID (`8202251`)              |
| `local_date`         | TEXT    | ISO 8601 datetime (local time)      |
| `temp`               | REAL    | Air temperature °C — NULL → `0`     |
| `dew_point_temp`     | REAL    | Dew point °C — NULL → `0`           |
| `humidex`            | INTEGER | Humidex — NULL → `0`                |
| `precip_amount`      | INTEGER | Precipitation mm — NULL → `0`       |
| `relative_humidity`  | INTEGER | Relative humidity % — NULL → `0`    |
| `station_pressure`   | REAL    | Station pressure kPa — NULL → `0`   |
| `visibility`         | REAL    | Visibility km — NULL → `0`          |
| `weather_eng_desc`   | TEXT    | English weather description         |
| `windchill`          | INTEGER | Wind chill — NULL → `0`             |
| `wind_direction`     | INTEGER | Wind direction degrees — NULL → `0` |
| `wind_speed`         | INTEGER | Wind speed km/h — NULL → `0`        |
| `insert_time`        | TEXT    | UTC timestamp of pipeline run       |

### `pipeline_runs` — Audit table (auto-created on first run)

| Column          | Type    | Notes |
|-----------------|---------|-------|
| `run_time`      | TEXT    | UTC timestamp of the run |
| `target_date`   | TEXT    | Date that was loaded |
| `rows_loaded`   | INTEGER | Number of rows written |
| `status`        | TEXT    | `SUCCESS` or `FAILED` |
| `error_message` | TEXT    | Error details if FAILED |

---

## Design Decisions

| Decision | Reason |
|----------|--------|
| `INSERT OR REPLACE` | Idempotent — re-running never creates duplicates. Safe to re-run after any failure |
| Single transaction | All 24 rows commit together. If one fails all roll back — table never in partial state |
| Pagination loop | Handles any volume of data automatically — not limited to 24 records |
| Skip today's date | Today is incomplete — only load full 24-hour days to avoid partial data |
| Pinned `requests==2.32.3` | Identical behaviour on every server. Unpinned versions can break silently |
| Virtual environment | Isolated dependencies — no conflicts with other system packages |
| Log to file and stdout | Visible in terminal now and saved to `pipeline.log` for later review |
| Validation step | Confirms data landed after every run. Catches silent failures immediately |
| Formatted table output | All 24 rows printed visually after every run for immediate confirmation |
| Audit table | Full history of every run. Answers operational questions without re-running |
| Exit code `1` on failure | Cron and monitoring tools detect failures automatically |
