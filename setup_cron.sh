#!/usr/bin/env bash
# =============================================================================
# setup_cron.sh
# =============================================================================
# Registers the pipeline as a daily cron job at 06:00 UTC.
#
# Why 06:00 UTC?
#   Ensures the previous day's full 24-hour dataset is available before
#   extraction begins. Running earlier risks loading an incomplete day.
#
# Usage:
#   bash setup_cron.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"
PIPELINE_SCRIPT="$SCRIPT_DIR/pipeline.py"
DB_PATH="$SCRIPT_DIR/yhz_db.sqlite"
LOG_FILE="$SCRIPT_DIR/pipeline.log"

# Check virtual environment exists before registering
if [ ! -f "$VENV_PYTHON" ]; then
    echo "[setup_cron] ERROR: Virtual environment not found."
    echo "             Please run 'bash install.sh' first."
    exit 1
fi

CRON_JOB="0 6 * * * $VENV_PYTHON $PIPELINE_SCRIPT --db $DB_PATH >> $LOG_FILE 2>&1"

# Only register if not already present — prevents duplicate cron entries
if crontab -l 2>/dev/null | grep -qF "$PIPELINE_SCRIPT"; then
    echo "[setup_cron] Cron job already registered. No changes made."
    exit 0
fi

(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "[setup_cron] Cron job registered successfully!"
echo "[setup_cron] Schedule:  daily at 06:00 UTC"
echo "[setup_cron] Log file:  $LOG_FILE"
echo ""
echo "  To verify: crontab -l"
