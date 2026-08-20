#!/usr/bin/env bash
#
# scripts/load_dbt_env.sh
#
# Load Snowflake connection settings for local dbt Core development.
#
# IMPORTANT:
#   Source this script. Do not execute it as a child process.
#
# Correct:
#
#   source scripts/load_dbt_env.sh
#
# The Snowflake password is read silently from the terminal and exported only
# into the current shell session. It is not written to .env, profiles.yml, Git,
# or any generated file.
#

# Detect accidental direct execution.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "ERROR: This script must be sourced."
    echo
    echo "Run:"
    echo "  source scripts/load_dbt_env.sh"
    exit 1
fi


# ============================================================
# ACT SNOWFLAKE DEVELOPMENT CONNECTION
# ============================================================
#
# These non-secret values match the existing working
# SNOWFLAKE_ACT_DEV connection.
# ============================================================

export SNOWFLAKE_ACCOUNT="JIJOAGV-FAB45884"
export SNOWFLAKE_USER="AVIGHNA01"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="ACT_DB"


# ============================================================
# DBT DEVELOPMENT SETTINGS
# ============================================================

export DBT_TARGET="${DBT_TARGET:-dev}"
export DBT_DEV_SCHEMA="${DBT_DEV_SCHEMA:-DBT_DEV}"


# ============================================================
# PASSWORD
# ============================================================
#
# Reuse an already-exported password if the current terminal
# already has one. Otherwise ask for it securely.
# ============================================================

if [[ -z "${SNOWFLAKE_PASSWORD:-}" ]]; then

    read -r -s \
        -p "Snowflake password for ${SNOWFLAKE_USER}: " \
        SNOWFLAKE_PASSWORD

    echo

    export SNOWFLAKE_PASSWORD
fi


# ============================================================
# NON-SECRET CONFIRMATION
# ============================================================

echo
echo "ACT dbt environment loaded"
echo "=========================="
echo "Account   : ${SNOWFLAKE_ACCOUNT}"
echo "User      : ${SNOWFLAKE_USER}"
echo "Role      : ${SNOWFLAKE_ROLE}"
echo "Warehouse : ${SNOWFLAKE_WAREHOUSE}"
echo "Database  : ${SNOWFLAKE_DATABASE}"
echo "Target    : ${DBT_TARGET}"
echo "Schema    : ${DBT_DEV_SCHEMA}"
echo "Password  : configured (hidden)"
echo