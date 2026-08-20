#!/usr/bin/env bash

# ============================================================================
# ACT Data Platform - Install Local Airflow
# File: scripts/install_airflow.sh
#
# Purpose:
#   Install Apache Airflow in a separate virtual environment
#   for local WSL2 development.
#
# Project environment:
#       .venv
#
# Airflow environment:
#       .airflow-venv
#
# Usage:
#       cd /mnt/c/Code/act-data-platform
#       bash scripts/install_airflow.sh
# ============================================================================

set -euo pipefail


# ============================================================================
# PROJECT ROOT
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"


# ============================================================================
# AIRFLOW VERSION
# ============================================================================

AIRFLOW_VERSION="3.3.1"

AIRFLOW_VENV="${PROJECT_ROOT}/.airflow-venv"


echo
echo "============================================================"
echo "ACT LOCAL AIRFLOW INSTALLATION"
echo "============================================================"
echo
echo "Project root    : ${PROJECT_ROOT}"
echo "Airflow version : ${AIRFLOW_VERSION}"
echo "Airflow venv    : ${AIRFLOW_VENV}"
echo


# ============================================================================
# PYTHON
# ============================================================================

PYTHON_BIN="${PYTHON_BIN:-python3}"


if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then

    echo "ERROR: ${PYTHON_BIN} was not found."

    exit 1

fi


PYTHON_VERSION="$(
    "${PYTHON_BIN}" -c \
    'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
)"


echo "Python version  : ${PYTHON_VERSION}"


# ============================================================================
# PYTHON VERSION VALIDATION
# ============================================================================

case "${PYTHON_VERSION}" in

    3.10|3.11|3.12|3.13)
        ;;

    *)
        echo
        echo "ERROR:"
        echo "Unsupported Python version for this ACT Airflow setup:"
        echo
        echo "  ${PYTHON_VERSION}"
        echo
        exit 1
        ;;

esac


# ============================================================================
# REQUIREMENTS
# ============================================================================

REQUIREMENTS_FILE="${PROJECT_ROOT}/requirements-airflow.txt"


if [[ ! -f "${REQUIREMENTS_FILE}" ]]; then

    echo
    echo "ERROR:"
    echo "requirements-airflow.txt was not found:"
    echo
    echo "  ${REQUIREMENTS_FILE}"
    echo

    exit 1

fi


# ============================================================================
# CREATE AIRFLOW VIRTUAL ENVIRONMENT
# ============================================================================

if [[ ! -d "${AIRFLOW_VENV}" ]]; then

    echo
    echo "Creating Airflow virtual environment..."

    "${PYTHON_BIN}" -m venv \
        "${AIRFLOW_VENV}"

else

    echo
    echo "Airflow virtual environment already exists."

fi


# ============================================================================
# ACTIVATE AIRFLOW VENV
# ============================================================================

# shellcheck disable=SC1091
source "${AIRFLOW_VENV}/bin/activate"


echo
echo "Active Python:"
which python

echo
echo "Active pip:"
which pip


# ============================================================================
# UPGRADE PIP TOOLING
# ============================================================================

echo
echo "Upgrading pip tooling..."

python -m pip install \
    --upgrade \
    pip \
    setuptools \
    wheel


# ============================================================================
# AIRFLOW CONSTRAINTS
# ============================================================================

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


echo
echo "Airflow constraints:"
echo
echo "  ${CONSTRAINT_URL}"
echo


# ============================================================================
# INSTALL AIRFLOW
# ============================================================================

echo
echo "Installing Apache Airflow ${AIRFLOW_VERSION}..."

python -m pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "${CONSTRAINT_URL}"


# ============================================================================
# INSTALL ACT RUNTIME DEPENDENCIES
# ============================================================================

echo
echo "Installing ACT Airflow runtime dependencies..."

python -m pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r "${REQUIREMENTS_FILE}"


# ============================================================================
# DEPENDENCY CHECK
# ============================================================================

echo
echo "Checking dependency consistency..."

python -m pip check


# ============================================================================
# VERSION CHECK
# ============================================================================

echo
echo "Installed Airflow version:"

airflow version


echo
echo "Installed Python version:"

python --version


# ============================================================================
# COMPLETE
# ============================================================================

echo
echo "============================================================"
echo "AIRFLOW INSTALLATION COMPLETED"
echo "============================================================"
echo
echo "Next command:"
echo
echo "  source scripts/load_airflow_env.sh"
echo