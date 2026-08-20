#!/usr/bin/env bash

# ============================================================================
# ACT Data Platform - Load dbt Snowflake Environment
# File: scripts/load_dbt_env.sh
#
# Purpose:
#   Load dbt Snowflake environment variables from the existing
#   Snowflake named connection:
#
#       ~/.snowflake/connections.toml
#       [SNOWFLAKE_ACT_DEV]
#
# This avoids duplicating Snowflake credentials in:
#
#       .env
#       profiles.yml
#       shell history
#
# Usage:
#
#   cd /mnt/c/Code/act-data-platform
#   source scripts/load_dbt_env.sh
#
# ============================================================================


# ---------------------------------------------------------------------------
# Ensure this script is sourced
# ---------------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "ERROR: This script must be sourced:"
    echo
    echo "  source scripts/load_dbt_env.sh"
    echo
    exit 1
fi


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

export SNOWFLAKE_CONNECTION_NAME="${SNOWFLAKE_CONNECTION_NAME:-SNOWFLAKE_ACT_DEV}"

SNOWFLAKE_CONNECTION_FILE="${HOME}/.snowflake/connections.toml"


# ---------------------------------------------------------------------------
# Validate connection file
# ---------------------------------------------------------------------------

if [[ ! -f "${SNOWFLAKE_CONNECTION_FILE}" ]]; then

    echo "ERROR: Snowflake connection file not found:"
    echo
    echo "  ${SNOWFLAKE_CONNECTION_FILE}"

    return 1
fi


# ---------------------------------------------------------------------------
# Read named connection securely
#
# Python prints shell-safe export commands.
# The password itself is NOT printed to the terminal because the output is
# consumed directly by eval.
# ---------------------------------------------------------------------------

ACT_DBT_EXPORTS="$(
python3 - "${SNOWFLAKE_CONNECTION_FILE}" "${SNOWFLAKE_CONNECTION_NAME}" <<'PY'
import shlex
import sys
import tomllib


connection_file = sys.argv[1]
connection_name = sys.argv[2]


with open(connection_file, "rb") as handle:
    config = tomllib.load(handle)


if connection_name not in config:
    raise SystemExit(
        f"Snowflake connection '{connection_name}' "
        f"not found in {connection_file}"
    )


connection = config[connection_name]


required = [
    "account",
    "user",
    "role",
    "warehouse",
    "database",
]


missing = [
    key
    for key in required
    if not str(connection.get(key, "")).strip()
]


if missing:
    raise SystemExit(
        "Missing required Snowflake connection values: "
        + ", ".join(missing)
    )


def export(name, value):
    if value is None:
        return

    print(
        f"export {name}={shlex.quote(str(value))}"
    )


export(
    "SNOWFLAKE_ACCOUNT",
    connection.get("account"),
)

export(
    "SNOWFLAKE_USER",
    connection.get("user"),
)

export(
    "SNOWFLAKE_ROLE",
    connection.get("role"),
)

export(
    "SNOWFLAKE_WAREHOUSE",
    connection.get("warehouse"),
)

export(
    "SNOWFLAKE_DATABASE",
    connection.get("database", "ACT_DB"),
)


# Password authentication
if connection.get("password"):

    export(
        "SNOWFLAKE_PASSWORD",
        connection["password"],
    )


# Optional authenticator
if connection.get("authenticator"):

    export(
        "SNOWFLAKE_AUTHENTICATOR",
        connection["authenticator"],
    )


# dbt development defaults
export(
    "DBT_TARGET",
    "dev",
)

export(
    "DBT_DEV_SCHEMA",
    "DBT_DEV",
)
PY
)"


if [[ $? -ne 0 ]]; then

    echo "ERROR: Unable to load Snowflake connection."

    unset ACT_DBT_EXPORTS

    return 1
fi


eval "${ACT_DBT_EXPORTS}"

unset ACT_DBT_EXPORTS


# ---------------------------------------------------------------------------
# Validate exported variables
# ---------------------------------------------------------------------------

REQUIRED_ENV_VARS=(
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
)


for VARIABLE_NAME in "${REQUIRED_ENV_VARS[@]}"; do

    if [[ -z "${!VARIABLE_NAME:-}" ]]; then

        echo "ERROR: ${VARIABLE_NAME} is empty."

        return 1
    fi

done


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------

echo "dbt Snowflake environment loaded."
echo
echo "Connection : ${SNOWFLAKE_CONNECTION_NAME}"
echo "Account    : ${SNOWFLAKE_ACCOUNT}"
echo "User       : ${SNOWFLAKE_USER}"
echo "Role       : ${SNOWFLAKE_ROLE}"
echo "Warehouse  : ${SNOWFLAKE_WAREHOUSE}"
echo "Database   : ${SNOWFLAKE_DATABASE}"
echo "dbt schema : ${DBT_DEV_SCHEMA}"
echo
echo "Password   : [loaded securely]"