#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="${VENV_DIR:-.venv}"
PYTHON="${PYTHON:-python3}"

if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "Error: $PYTHON is not installed or not on PATH."
  exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment in $VENV_DIR..."
  "$PYTHON" -m venv "$VENV_DIR"
else
  echo "Virtual environment already exists at $VENV_DIR."
fi

echo "Installing build tools and project dependencies..."
"$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
"$VENV_DIR/bin/python" -m pip install .

echo
printf 'Environment ready. Activate it with:\n  source "%s/bin/activate"\n' "$VENV_DIR"
