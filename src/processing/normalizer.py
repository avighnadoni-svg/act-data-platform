# src/processing/normalizer.py

from dataclasses import dataclass

import pandas as pd

from config.endpoints import ENDPOINTS

from src.common.exceptions import (
    ConfigurationError,
    DataValidationError,
)

from src.common.logging_config import get_logger


logger = get_logger(__name__)


# ============================================================
# NORMALIZATION RESULT
# ============================================================

@dataclass
class NormalizationResult:
    """
    Result returned after successful normalization.
    """

    entity_name: str
    dataframe: pd.DataFrame
    record_count: int
    run_id: str


# ============================================================
# COMMON HELPERS
# ============================================================

def _validate_entity(
    entity_name: str,
) -> None:

    if entity_name not in ENDPOINTS:

        raise ConfigurationError(
            f"Unknown entity={entity_name}"
        )


def _clean_string_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Clean all text/object columns.

    Example:

        "  severe " -> "severe"

    Empty strings become pandas NA.
    """

    output = df.copy()

    for column in output.columns:

        if (
            pd.api.types.is_object_dtype(
                output[column]
            )
            or pd.api.types.is_string_dtype(
                output[column]
            )
        ):

            output[column] = (
                output[column]
                .astype("string")
                .str.strip()
            )

            output[column] = (
                output[column]
                .replace(
                    "",
                    pd.NA,
                )
            )

    return output


def _convert_integer(
    df: pd.DataFrame,
    column: str,
) -> None:
    """
    Convert column to nullable integer.
    """

    if column not in df.columns:
        return

    converted = pd.to_numeric(
        df[column],
        errors="coerce",
    )

    invalid_mask = (
        converted.isna()
        & df[column].notna()
    )

    if invalid_mask.any():

        invalid_values = (
            df.loc[
                invalid_mask,
                column,
            ]
            .astype(str)
            .tolist()
        )

        raise DataValidationError(
            (
                f"Invalid integer values "
                f"column={column} "
                f"values={invalid_values}"
            )
        )

    df[column] = (
        converted.astype("Int64")
    )


def _convert_float(
    df: pd.DataFrame,
    column: str,
) -> None:
    """
    Convert column to nullable floating point.
    """

    if column not in df.columns:
        return

    converted = pd.to_numeric(
        df[column],
        errors="coerce",
    )

    invalid_mask = (
        converted.isna()
        & df[column].notna()
    )

    if invalid_mask.any():

        invalid_values = (
            df.loc[
                invalid_mask,
                column,
            ]
            .astype(str)
            .tolist()
        )

        raise DataValidationError(
            (
                f"Invalid numeric values "
                f"column={column} "
                f"values={invalid_values}"
            )
        )

    df[column] = (
        converted.astype("Float64")
    )


def _convert_date(
    df: pd.DataFrame,
    column: str,
) -> None:
    """
    Normalize date columns into YYYY-MM-DD.

    Missing dates remain NULL.
    """

    if column not in df.columns:
        return

    original = df[column].copy()

    converted = pd.to_datetime(
        original,
        errors="coerce",
    )

    invalid_mask = (
        converted.isna()
        & original.notna()
    )

    if invalid_mask.any():

        invalid_values = (
            original.loc[
                invalid_mask
            ]
            .astype(str)
            .tolist()
        )

        raise DataValidationError(
            (
                f"Invalid date values "
                f"column={column} "
                f"values={invalid_values}"
            )
        )

    df[column] = (
        converted
        .dt.strftime(
            "%Y-%m-%d"
        )
        .astype("string")
    )


def _convert_boolean(
    df: pd.DataFrame,
    column: str,
) -> None:
    """
    Convert common boolean representations
    into Pandas nullable boolean.
    """

    if column not in df.columns:
        return

    mapping = {
        "TRUE": True,
        "FALSE": False,
        "Y": True,
        "N": False,
        "YES": True,
        "NO": False,
        "1": True,
        "0": False,
    }

    original = (
        df[column]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    converted = (
        original.map(
            mapping
        )
    )

    invalid_mask = (
        converted.isna()
        & original.notna()
    )

    if invalid_mask.any():

        invalid_values = (
            original.loc[
                invalid_mask
            ]
            .dropna()
            .unique()
            .tolist()
        )

        raise DataValidationError(
            (
                f"Invalid boolean values "
                f"column={column} "
                f"values={invalid_values}"
            )
        )

    df[column] = (
        converted.astype("boolean")
    )


def _uppercase(
    df: pd.DataFrame,
    column: str,
) -> None:

    if column not in df.columns:
        return

    df[column] = (
        df[column]
        .astype("string")
        .str.strip()
        .str.upper()
    )


# ============================================================
# STUDY
# ============================================================

def _normalize_study(
    df: pd.DataFrame,
) -> None:

    _convert_integer(
        df,
        "target_subjects",
    )

    _uppercase(
        df,
        "phase",
    )


# ============================================================
# SITE
# ============================================================

def _normalize_site(
    df: pd.DataFrame,
) -> None:

    _convert_integer(
        df,
        "target_enrollment",
    )

    _uppercase(
        df,
        "country",
    )


# ============================================================
# SUBJECT
# ============================================================

def _normalize_subject(
    df: pd.DataFrame,
) -> None:

    _convert_integer(
        df,
        "age",
    )

    _convert_date(
        df,
        "enrollment_date",
    )

    _uppercase(
        df,
        "gender",
    )

    _uppercase(
        df,
        "status",
    )


# ============================================================
# VISIT
# ============================================================

def _normalize_visit(
    df: pd.DataFrame,
) -> None:

    _convert_date(
        df,
        "planned_date",
    )

    _convert_date(
        df,
        "actual_date",
    )


# ============================================================
# ADVERSE EVENT
# ============================================================

def _normalize_adverse_event(
    df: pd.DataFrame,
) -> None:

    _uppercase(
        df,
        "severity",
    )

    _uppercase(
        df,
        "serious",
    )

    _uppercase(
        df,
        "processing_priority",
    )

    _convert_date(
        df,
        "event_date",
    )

    _convert_date(
        df,
        "reported_date",
    )

    _convert_boolean(
        df,
        "requires_safety_review",
    )


# ============================================================
# LAB RESULT
# ============================================================

def _normalize_lab_result(
    df: pd.DataFrame,
) -> None:

    _convert_float(
        df,
        "result_value",
    )

    _convert_float(
        df,
        "normal_low",
    )

    _convert_float(
        df,
        "normal_high",
    )

    _uppercase(
        df,
        "interpretation",
    )

    _convert_boolean(
        df,
        "abnormal",
    )


# ============================================================
# PROTOCOL DEVIATION
# ============================================================

def _normalize_protocol_deviation(
    df: pd.DataFrame,
) -> None:

    _uppercase(
        df,
        "severity",
    )


# ============================================================
# DATA QUERY
# ============================================================

def _normalize_data_query(
    df: pd.DataFrame,
) -> None:

    _convert_date(
        df,
        "opened_date",
    )

    _convert_date(
        df,
        "resolved_date",
    )

    _uppercase(
        df,
        "status",
    )


# ============================================================
# ENTITY ROUTER
# ============================================================

def _run_entity_normalization(
    entity_name: str,
    df: pd.DataFrame,
) -> None:

    if entity_name == "study":

        _normalize_study(df)

    elif entity_name == "site":

        _normalize_site(df)

    elif entity_name == "subject":

        _normalize_subject(df)

    elif entity_name == "visit":

        _normalize_visit(df)

    elif entity_name == "adverse_event":

        _normalize_adverse_event(df)

    elif entity_name == "lab_result":

        _normalize_lab_result(df)

    elif entity_name == "protocol_deviation":

        _normalize_protocol_deviation(df)

    elif entity_name == "data_query":

        _normalize_data_query(df)


# ============================================================
# AUDIT COLUMNS
# ============================================================

def _add_audit_columns(
    df: pd.DataFrame,
    entity_name: str,
    run_id: str,
) -> None:
    """
    Add ingestion metadata.

    These columns help trace a record back
    to the Airflow ingestion run.
    """

    df["source_system"] = (
        "RAVE_MOCK"
    )

    df["source_entity"] = (
        entity_name
    )

    df["dag_run_id"] = (
        run_id
    )

    df["ingested_at"] = (
        pd.Timestamp.now(
            tz="UTC"
        )
    )


# ============================================================
# PUBLIC FUNCTION
# ============================================================

def normalize_dataframe(
    entity_name: str,
    df: pd.DataFrame,
    run_id: str,
) -> NormalizationResult:
    """
    Standardize validated records before
    writing them to the landing layer.

    Input:
        Validated Pandas DataFrame

    Output:
        Normalized Pandas DataFrame
    """

    _validate_entity(
        entity_name
    )

    logger.info(
        (
            "entity=%s "
            "normalization_started "
            "record_count=%s "
            "run_id=%s"
        ),
        entity_name,
        len(df),
        run_id,
    )

    # ========================================================
    # EMPTY BATCH
    # ========================================================

    if df.empty:

        logger.info(
            (
                "entity=%s "
                "normalization_skipped "
                "reason=no_records"
            ),
            entity_name,
        )

        return NormalizationResult(
            entity_name=entity_name,
            dataframe=df.copy(),
            record_count=0,
            run_id=run_id,
        )

    # ========================================================
    # COPY
    # ========================================================

    normalized_df = (
        df.copy()
    )

    # ========================================================
    # COMMON STRING CLEANING
    # ========================================================

    normalized_df = (
        _clean_string_columns(
            normalized_df
        )
    )

    # ========================================================
    # WATERMARK
    # ========================================================

    watermark_field = (
        ENDPOINTS[
            entity_name
        ]["watermark_field"]
    )

    normalized_df[
        watermark_field
    ] = pd.to_datetime(
        normalized_df[
            watermark_field
        ],
        utc=True,
        errors="raise",
    )

    # ========================================================
    # ENTITY-SPECIFIC NORMALIZATION
    # ========================================================

    _run_entity_normalization(
        entity_name,
        normalized_df,
    )

    # ========================================================
    # AUDIT COLUMNS
    # ========================================================

    _add_audit_columns(
        normalized_df,
        entity_name,
        run_id,
    )

    logger.info(
        (
            "entity=%s "
            "normalization_completed "
            "record_count=%s "
            "column_count=%s "
            "run_id=%s"
        ),
        entity_name,
        len(normalized_df),
        len(normalized_df.columns),
        run_id,
    )

    return NormalizationResult(
        entity_name=entity_name,
        dataframe=normalized_df,
        record_count=len(
            normalized_df
        ),
        run_id=run_id,
    )