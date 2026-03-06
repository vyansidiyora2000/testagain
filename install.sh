#!/usr/bin/env bash
# =============================================================================
# install.sh
# =============================================================================
# Sets up the Python virtual environment and installs all dependencies.
# Run this ONCE on any new server before executing the pipeline.
#
# Requirements:
#   - Python 3.11 or higher
#
# Usage:
#   bash install.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
REQUIREMENTS="$SCRIPT_DIR/requirements.txt"

echo "[install] Checking Python version..."
python3 --version

echo "[install] Creating virtual environment at $VENV_DIR ..."
python3 -m venv "$VENV_DIR"

echo "[install] Installing dependencies..."
"$VENV_DIR/bin/pip" install --upgrade pip --quiet
"$VENV_DIR/bin/pip" install -r "$REQUIREMENTS" --quiet

echo ""
echo "[install] Installation complete!"
echo ""
echo "  To run the pipeline:     .venv/bin/python pipeline.py"
echo "  To run with debug logs:  .venv/bin/python pipeline.py --log-level DEBUG"
echo "  To schedule daily runs:  bash setup_cron.sh"
