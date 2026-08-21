#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${REPO_ROOT}"

if [[ ! -d ".dashboard-venv" ]]; then
    echo "Missing .dashboard-venv"
    echo "Create it with:"
    echo "  python3 -m venv .dashboard-venv"
    echo "  source .dashboard-venv/bin/activate"
    echo "  pip install -r requirements-dashboard.txt"
    exit 1
fi

source .dashboard-venv/bin/activate

export SNOWFLAKE_CONNECTION_NAME="${SNOWFLAKE_CONNECTION_NAME:-SNOWFLAKE_ACT_DEV}"

exec streamlit run \
    dashboard/operational_dashboard.py \
    --server.address 0.0.0.0 \
    --server.port 8501