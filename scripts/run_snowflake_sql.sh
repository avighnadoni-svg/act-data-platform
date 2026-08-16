#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# ACT Data Platform - Snowflake SQL Runner
# File: scripts/run_snowflake_sql.sh
#
# Usage:
#   ./scripts/run_snowflake_sql.sh snowflake/sql/001_create_foundation.sql
#
# Optional connection override:
#   SNOWFLAKE_CONNECTION_NAME=act_dev \
#     ./scripts/run_snowflake_sql.sh snowflake/sql/001_create_foundation.sql
#
# The Snowflake CLI connection must already be configured locally.
# No Snowflake password or token is stored in this repository.
# ============================================================================

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <sql-file> [additional snow sql arguments]"
    exit 1
fi

SQL_FILE="$1"
shift

if [[ ! -f "$SQL_FILE" ]]; then
    echo "ERROR: SQL file not found: $SQL_FILE"
    exit 1
fi

if ! command -v snow >/dev/null 2>&1; then
    echo "ERROR: Snowflake CLI ('snow') is not installed or not on PATH."
    echo "Run: snow --version"
    exit 1
fi

CONNECTION_NAME="${SNOWFLAKE_CONNECTION_NAME:-default}"

echo "Executing Snowflake SQL file"
echo "  connection : ${CONNECTION_NAME}"
echo "  file       : ${SQL_FILE}"

echo
snow sql \
    --connection "${CONNECTION_NAME}" \
    --filename "${SQL_FILE}" \
    "$@"

echo
echo "SUCCESS: ${SQL_FILE}"
