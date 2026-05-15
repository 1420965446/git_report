#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

VENV_DIR="$PWD/.venv"
PYTHON_EXE="$VENV_DIR/bin/python3"
URL="http://127.0.0.1:8000"

echo "[1/4] Checking virtual environment..."
if [ ! -x "$PYTHON_EXE" ]; then
  echo "Creating virtual environment..."
  python3 -m venv "$VENV_DIR"
fi

echo "[2/4] Installing dependencies..."
"$PYTHON_EXE" -m pip install --upgrade pip
"$PYTHON_EXE" -m pip install -r requirements.txt

echo "[3/4] Opening browser..."
( sleep 3; open "$URL" ) >/dev/null 2>&1 &

echo "[4/4] Starting web app..."
exec "$PYTHON_EXE" -m uvicorn app.main:app --host 127.0.0.1 --port 8000
