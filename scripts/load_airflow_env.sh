#!/usr/bin/env bash

# ============================================================================
# ACT Data Platform - Load Local Airflow Environment
# File: scripts/load_airflow_env.sh
#
# Usage:
#
#   cd /mnt/c/Code/act-data-platform
#   source scripts/load_airflow_env.sh
#
# Purpose:
#
#   1. Activate the dedicated Airflow virtual environment
#   2. Load ACT .env configuration
#   3. Configure AIRFLOW_HOME
#   4. Point Airflow to this repository's dags/ directory
#   5. Add the ACT project root to PYTHONPATH
# ============================================================================


# ============================================================================
# MUST BE SOURCED
# ============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then

    echo "ERROR: This script must be sourced."
    echo
    echo "Use:"
    echo
    echo "  source scripts/load_airflow_env.sh"
    echo

    exit 1

fi


# ============================================================================
# PROJECT ROOT
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"


# ============================================================================
# AIRFLOW VIRTUAL ENVIRONMENT
# ============================================================================

AIRFLOW_VENV="${PROJECT_ROOT}/.airflow-venv"


if [[ ! -f "${AIRFLOW_VENV}/bin/activate" ]]; then

    echo
    echo "ERROR: Airflow virtual environment does not exist:"
    echo
    echo "  ${AIRFLOW_VENV}"
    echo
    echo "Run:"
    echo
    echo "  bash scripts/install_airflow.sh"
    echo

    return 1

fi


# ============================================================================
# ACTIVATE AIRFLOW ENVIRONMENT
# ============================================================================

# shellcheck disable=SC1091
source "${AIRFLOW_VENV}/bin/activate"


# ============================================================================
# LOAD PROJECT .ENV
# ============================================================================

ENV_FILE="${PROJECT_ROOT}/.env"


if [[ ! -f "${ENV_FILE}" ]]; then

    echo
    echo "ERROR: ACT .env file was not found:"
    echo
    echo "  ${ENV_FILE}"
    echo

    return 1

fi


set -a

# shellcheck disable=SC1090
source "${ENV_FILE}"

set +a


# ============================================================================
# AIRFLOW HOME
# ============================================================================
#
# Airflow metadata stays inside the WSL Linux filesystem.
#
# Do NOT place the SQLite metadata database under /mnt/c because Windows
# filesystem semantics can make SQLite/Airflow unnecessarily slow.
# ============================================================================

export AIRFLOW_HOME="${HOME}/.airflow-act"


# ============================================================================
# DAG LOCATION
# ============================================================================

export AIRFLOW__CORE__DAGS_FOLDER="${PROJECT_ROOT}/dags"


# ============================================================================
# LOG LOCATION
# ============================================================================

export AIRFLOW__LOGGING__BASE_LOG_FOLDER="${AIRFLOW_HOME}/logs"


# ============================================================================
# AIRFLOW CORE SETTINGS
# ============================================================================

export AIRFLOW__CORE__LOAD_EXAMPLES="False"

export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="False"


# ============================================================================
# EXECUTOR
# ============================================================================
#
# LocalExecutor is appropriate for our local development pipeline.
# ============================================================================

export AIRFLOW__CORE__EXECUTOR="LocalExecutor"


# ============================================================================
# PROJECT PYTHON PATH
# ============================================================================
#
# Required so DAGs can import:
#
#   src.api
#   src.storage
#   src.snowflake
#   config
# ============================================================================

case ":${PYTHONPATH:-}:" in

    *":${PROJECT_ROOT}:"*)
        ;;

    *)
        if [[ -n "${PYTHONPATH:-}" ]]; then

            export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"

        else

            export PYTHONPATH="${PROJECT_ROOT}"

        fi
        ;;

esac


# ============================================================================
# ACT STORAGE DEFAULTS
# ============================================================================

export STORAGE_BACKEND="${STORAGE_BACKEND:-local}"

export LOCAL_STORAGE_ROOT="${LOCAL_STORAGE_ROOT:-data/raw}"


# ============================================================================
# SNOWFLAKE
# ============================================================================

export SNOWFLAKE_CONNECTION_NAME="${SNOWFLAKE_CONNECTION_NAME:-SNOWFLAKE_ACT_DEV}"


# ============================================================================
# CREATE RUNTIME DIRECTORIES
# ============================================================================

mkdir -p "${AIRFLOW_HOME}"

mkdir -p "${AIRFLOW_HOME}/logs"


# ============================================================================
# VALIDATION
# ============================================================================

if ! command -v python >/dev/null 2>&1; then

    echo
    echo "ERROR: Python is unavailable after activating Airflow environment."

    return 1

fi


if ! command -v airflow >/dev/null 2>&1; then

    echo
    echo "ERROR: Airflow executable was not found."
    echo
    echo "Run:"
    echo
    echo "  bash scripts/install_airflow.sh"
    echo

    return 1

fi


# ============================================================================
# SUCCESS
# ============================================================================

echo
echo "============================================================"
echo "ACT AIRFLOW ENVIRONMENT"
echo "============================================================"
echo
echo "Project root         : ${PROJECT_ROOT}"
echo "Airflow home         : ${AIRFLOW_HOME}"
echo "DAG folder           : ${AIRFLOW__CORE__DAGS_FOLDER}"
echo "Executor             : ${AIRFLOW__CORE__EXECUTOR}"
echo "Storage backend      : ${STORAGE_BACKEND}"
echo "Local storage root   : ${LOCAL_STORAGE_ROOT}"
echo "Snowflake connection : ${SNOWFLAKE_CONNECTION_NAME}"
echo "Rave API             : ${RAVE_API_BASE_URL:-NOT_SET}"
echo
echo "Python               : $(command -v python)"
echo "Airflow              : $(command -v airflow)"
echo "============================================================"
echo